#pragma once

#include "bwm/core/error.hpp"

#if BWM_HAVE_STD_EXPECTED && __has_include(<expected>)
#include <expected>
#else
#if __has_include(<tl/expected.hpp>)
#include <tl/expected.hpp>
#else
#include <type_traits>
#include <utility>
#include <variant>
#endif
#endif

namespace bwm {

#if BWM_HAVE_STD_EXPECTED && __has_include(<expected>)

template <class T>
using Expected = std::expected<T, Error>;

template <class E>
using unexpected = std::unexpected<E>;

#else

#if __has_include(<tl/expected.hpp>)

template <class T>
using Expected = tl::expected<T, Error>;

template <class E>
using unexpected = tl::unexpected<E>;

#else

template <class E>
class unexpected {
 public:
  explicit unexpected(E e) : error_(std::move(e)) {}
  const E& error() const & { return error_; }
  E& error() & { return error_; }
  E&& error() && { return std::move(error_); }

 private:
  E error_;
};

template <class T, class E>
class BasicExpected {
 public:
  BasicExpected(const T& v) : has_(true), storage_(v) {}
  BasicExpected(T&& v) : has_(true), storage_(std::move(v)) {}

  template <class G>
  BasicExpected(const unexpected<G>& u) : has_(false), storage_(E{u.error()}) {}

  template <class G>
  BasicExpected(unexpected<G>&& u) : has_(false), storage_(E{std::move(u).error()}) {}

  bool has_value() const { return has_; }
  explicit operator bool() const { return has_; }

  T& value() & { return std::get<T>(storage_); }
  const T& value() const & { return std::get<T>(storage_); }
  T&& value() && { return std::move(std::get<T>(storage_)); }

  E& error() & { return std::get<E>(storage_); }
  const E& error() const & { return std::get<E>(storage_); }
  E&& error() && { return std::move(std::get<E>(storage_)); }

  T& operator*() { return value(); }
  const T& operator*() const { return value(); }
  T* operator->() { return &value(); }
  const T* operator->() const { return &value(); }

 private:
  bool has_;
  std::variant<T, E> storage_;
};

template <class E>
class BasicExpected<void, E> {
 public:
  BasicExpected() : has_(true), error_{} {}

  template <class G>
  BasicExpected(const unexpected<G>& u) : has_(false), error_(E{u.error()}) {}

  template <class G>
  BasicExpected(unexpected<G>&& u) : has_(false), error_(E{std::move(u).error()}) {}

  bool has_value() const { return has_; }
  explicit operator bool() const { return has_; }

  E& error() & { return error_; }
  const E& error() const & { return error_; }
  E&& error() && { return std::move(error_); }

 private:
  bool has_;
  E error_;
};

template <class T>
using Expected = BasicExpected<T, Error>;

#endif

#endif

}  // namespace bwm

#if !(BWM_HAVE_STD_EXPECTED && __has_include(<expected>))
namespace std {
template <class E>
using unexpected = ::bwm::unexpected<E>;
}  // namespace std
#endif
