#pragma once

#include <exception>
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

class Error : public std::exception {
  ErrorCode code_{ErrorCode::Internal};
  std::string message_{};

public:
  Error() = default;
  Error(ErrorCode c, std::string m) : code_(c), message_(std::move(m)) {}
  explicit Error(std::string m) : message_(std::move(m)) {}

  const char *what() const noexcept override { return message_.c_str(); }

  ErrorCode code() const noexcept { return code_; }
};

} // namespace bwm
