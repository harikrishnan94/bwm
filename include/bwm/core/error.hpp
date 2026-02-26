#pragma once

#include <string>
#include <utility>

namespace bwm {

enum class ErrorCode {
  InvalidArgument,
  IoError,
  CodecError,
  QueueFull,
  Unsupported,
  Internal,
};

struct Error {
  ErrorCode code{};
  std::string message{};

  Error() = default;
  Error(ErrorCode c, std::string m) : code(c), message(std::move(m)) {}
  explicit Error(std::string m) : code(ErrorCode::Internal), message(std::move(m)) {}
};

}  // namespace bwm
