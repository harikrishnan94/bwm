#include "bwm/codec/codec.hpp"

#include <exception>

#include "app/config_types.hpp"
#include "codec/runtime_codec.hpp"

namespace bwm {
namespace {

app::Algo to_app_algo(CodecId id) {
  switch (id) {
    case CodecId::None:
      return app::Algo::None;
    case CodecId::Lz4:
      return app::Algo::Lz4;
    case CodecId::Zstd:
      return app::Algo::Zstd;
  }
  return app::Algo::None;
}

const char* codec_name(CodecId id) {
  switch (id) {
    case CodecId::None:
      return "none";
    case CodecId::Lz4:
      return "lz4";
    case CodecId::Zstd:
      return "zstd";
  }
  return "unknown";
}

class CodecAdapter final : public ICodec {
 public:
  explicit CodecAdapter(CodecParams params)
      : id_(params.id), impl_(to_app_algo(params.id), params.level) {}

  CodecId id() const noexcept override { return id_; }
  const char* name() const noexcept override { return codec_name(id_); }

  size_t max_compressed_size(size_t raw_size) const noexcept override {
    return impl_.max_compressed_size(raw_size);
  }

  Expected<size_t> compress(std::span<const std::byte> raw,
                            std::span<std::byte> out) noexcept override {
    auto raw_u8 =
        std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(raw.data()), raw.size());
    auto out_u8 = std::span<uint8_t>(reinterpret_cast<uint8_t*>(out.data()), out.size());
    return impl_.compress(raw_u8, out_u8);
  }

  Expected<size_t> decompress(std::span<const std::byte> comp,
                              std::span<std::byte> out,
                              size_t expected_raw_size) noexcept override {
    auto comp_u8 =
        std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(comp.data()), comp.size());
    auto out_u8 = std::span<uint8_t>(reinterpret_cast<uint8_t*>(out.data()), out.size());
    return impl_.decompress(comp_u8, out_u8, expected_raw_size);
  }

 private:
  CodecId id_;
  app::Codec impl_;
};

}  // namespace

Expected<std::unique_ptr<ICodec>> make_codec(const CodecParams& params) noexcept {
  if (params.id != CodecId::None && params.id != CodecId::Lz4 && params.id != CodecId::Zstd) {
    return std::unexpected(Error{ErrorCode::InvalidArgument, "invalid codec id"});
  }

  try {
    return std::unique_ptr<ICodec>(new CodecAdapter(params));
  } catch (const std::exception& ex) {
    return std::unexpected(Error{ErrorCode::Internal, ex.what()});
  } catch (...) {
    return std::unexpected(Error{ErrorCode::Internal, "failed to allocate codec"});
  }
}

}  // namespace bwm
