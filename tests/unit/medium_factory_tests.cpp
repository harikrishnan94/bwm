#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <format>
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
    std::cerr << std::format("make_disk_medium failed\n");
    return false;
  }
  if (medium->kind() != bwm::MediumKind::Disk) {
    std::cerr << std::format("disk medium kind mismatch\n");
    return false;
  }

  bwm::WriteOpenParams wop{};
  wop.segment_id = 0;
  wop.truncate = true;

  std::unique_ptr<bwm::IMediumWriter> writer;
  try {
    writer = medium->open_writer(wop);
  } catch (const bwm::Error &e) {
    std::cerr << std::format("disk open_writer failed: {}\n", e.what());
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

  try {
    writer->append_chunk(hdr, payload);
    writer->sync();
    writer->close();
  } catch (const bwm::Error &e) {
    std::cerr << std::format("writer operation failed: {}\n", e.what());
    return false;
  }

  bwm::ReadOpenParams rop{};
  rop.segment_paths.push_back((out_dir / "segment_0.dat").string());

  std::unique_ptr<bwm::IMediumReader> reader;
  bwm::OwnedChunk chunk{};
  try {
    reader = medium->open_reader(rop);
    chunk = reader->read_chunk_by_index(0, 0);
  } catch (const bwm::Error &e) {
    std::cerr << std::format("reader operation failed: {}\n", e.what());
    return false;
  }

  if (chunk.storage.size() != payload.size()) {
    std::cerr << std::format("payload size mismatch\n");
    return false;
  }
  for (size_t i = 0; i < payload.size(); ++i) {
    if (chunk.storage[i] != payload[i]) {
      std::cerr << std::format("payload data mismatch at index {}\n", i);
      return false;
    }
  }

  try {
    reader->close();
  } catch (const bwm::Error &e) {
    std::cerr << std::format("close reader failed: {}\n", e.what());
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
    std::cerr << std::format("make_tcp_sender_medium failed\n");
    return false;
  }
  if (sender->kind() != bwm::MediumKind::Tcp) {
    std::cerr << std::format("sender medium kind mismatch\n");
    return false;
  }

  bool sender_read_unsupported = false;
  try {
    static_cast<void>(sender->open_reader(bwm::ReadOpenParams{}));
  } catch (const bwm::Error &e) {
    sender_read_unsupported = (e.code() == bwm::ErrorCode::Unsupported);
  }
  if (!sender_read_unsupported) {
    std::cerr << std::format("tcp sender open_reader should be unsupported\n");
    return false;
  }

  auto receiver = bwm::make_tcp_receiver_medium(rc);
  if (!receiver) {
    std::cerr << std::format("make_tcp_receiver_medium failed\n");
    return false;
  }

  bool receiver_write_unsupported = false;
  try {
    static_cast<void>(receiver->open_writer(bwm::WriteOpenParams{}));
  } catch (const bwm::Error &e) {
    receiver_write_unsupported = (e.code() == bwm::ErrorCode::Unsupported);
  }
  if (!receiver_write_unsupported) {
    std::cerr <<
        std::format("tcp receiver open_writer should be unsupported\n");
    return false;
  }

  return true;
}

} // namespace

int main() {
  if (!test_disk_medium_roundtrip()) {
    return 1;
  }
  if (!test_tcp_medium_contracts()) {
    return 1;
  }
  return 0;
}
