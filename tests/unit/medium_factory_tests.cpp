#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <iostream>
#include <string>
#include <vector>

#include "bwm/core/error.hpp"
#include "bwm/medium/medium.hpp"

namespace {

bool test_disk_medium_roundtrip() {
  const std::filesystem::path out_dir = "./bwm_test_medium_out";
  std::error_code ec;
  std::filesystem::remove_all(out_dir, ec);
  std::filesystem::create_directories(out_dir, ec);

  bwm::RunConfig rc{};
  rc.medium = bwm::MediumKind::Disk;
  rc.output_dir = out_dir.string();

  auto medium = bwm::make_disk_medium(rc);
  if (!medium) {
    std::cerr << "make_disk_medium failed\n";
    return false;
  }
  if ((*medium)->kind() != bwm::MediumKind::Disk) {
    std::cerr << "disk medium kind mismatch\n";
    return false;
  }

  bwm::WriteOpenParams wop{};
  wop.segment_id = 0;
  wop.truncate = true;

  auto writer = (*medium)->open_writer(wop);
  if (!writer) {
    std::cerr << "disk open_writer failed: " << writer.error().message << "\n";
    return false;
  }

  std::vector<std::byte> payload(128);
  for (size_t i = 0; i < payload.size(); ++i) {
    payload[i] = static_cast<std::byte>(i);
  }

  bwm::ChunkHeader hdr{};
  hdr.raw_size = static_cast<uint32_t>(payload.size());
  hdr.comp_size = static_cast<uint32_t>(payload.size());
  hdr.checksum = 0;
  hdr.algo_id = static_cast<uint8_t>(bwm::CodecId::None);

  auto append = (*writer)->append_chunk(hdr, payload);
  if (!append) {
    std::cerr << "append_chunk failed: " << append.error().message << "\n";
    return false;
  }
  auto sync = (*writer)->sync();
  if (!sync) {
    std::cerr << "sync failed: " << sync.error().message << "\n";
    return false;
  }
  auto close_w = (*writer)->close();
  if (!close_w) {
    std::cerr << "close writer failed: " << close_w.error().message << "\n";
    return false;
  }

  bwm::ReadOpenParams rop{};
  rop.segment_paths.push_back((out_dir / "segment_0.dat").string());

  auto reader = (*medium)->open_reader(rop);
  if (!reader) {
    std::cerr << "disk open_reader failed: " << reader.error().message << "\n";
    return false;
  }

  auto chunk = (*reader)->read_chunk_by_index(0, 0);
  if (!chunk) {
    std::cerr << "read chunk failed: " << chunk.error().message << "\n";
    return false;
  }
  if (chunk->storage.size() != payload.size()) {
    std::cerr << "payload size mismatch\n";
    return false;
  }
  for (size_t i = 0; i < payload.size(); ++i) {
    if (chunk->storage[i] != payload[i]) {
      std::cerr << "payload data mismatch at index " << i << "\n";
      return false;
    }
  }

  auto close_r = (*reader)->close();
  if (!close_r) {
    std::cerr << "close reader failed: " << close_r.error().message << "\n";
    return false;
  }

  std::filesystem::remove_all(out_dir, ec);
  return true;
}

bool test_tcp_medium_contracts() {
  bwm::RunConfig rc{};
  rc.medium = bwm::MediumKind::Tcp;
  rc.tcp_port = 9191;

  auto sender = bwm::make_tcp_sender_medium(rc);
  if (!sender) {
    std::cerr << "make_tcp_sender_medium failed\n";
    return false;
  }
  if ((*sender)->kind() != bwm::MediumKind::Tcp) {
    std::cerr << "sender medium kind mismatch\n";
    return false;
  }

  auto sender_reader = (*sender)->open_reader(bwm::ReadOpenParams{});
  if (sender_reader || sender_reader.error().code != bwm::ErrorCode::Unsupported) {
    std::cerr << "tcp sender open_reader should be unsupported\n";
    return false;
  }

  auto receiver = bwm::make_tcp_receiver_medium(rc);
  if (!receiver) {
    std::cerr << "make_tcp_receiver_medium failed\n";
    return false;
  }

  auto receiver_writer = (*receiver)->open_writer(bwm::WriteOpenParams{});
  if (receiver_writer || receiver_writer.error().code != bwm::ErrorCode::Unsupported) {
    std::cerr << "tcp receiver open_writer should be unsupported\n";
    return false;
  }

  return true;
}

}  // namespace

int main() {
  if (!test_disk_medium_roundtrip()) {
    return 1;
  }
  if (!test_tcp_medium_contracts()) {
    return 1;
  }
  return 0;
}
