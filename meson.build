project(
  'bwm',
  'cpp',
  version: '0.1.0',
  meson_version: '>=1.5.0',
  default_options: [
    'cpp_std=c++23',
    'warning_level=3',
  ],
)

cpp = meson.get_compiler('cpp')

std_expected_ok = cpp.compiles(
  '''
  #include <expected>
  std::expected<int, int> x = 1;
  int main() {
    return x.has_value() ? 0 : 1;
  }
  ''',
  name: 'std::expected availability',
  required: false,
)
if std_expected_ok
  add_project_arguments('-DBWM_HAVE_STD_EXPECTED=1', language: 'cpp')
else
  add_project_arguments('-DBWM_HAVE_STD_EXPECTED=0', language: 'cpp')
endif

lz4_dep = dependency('liblz4', required: false)
if not lz4_dep.found()
  lz4_dep = dependency('lz4', fallback: ['lz4', 'lz4_dep'], required: true)
endif
zstd_dep = dependency('libzstd', fallback: ['zstd', 'zstd_dep'], required: false)
if not zstd_dep.found()
  zstd_dep = dependency('zstd', fallback: ['zstd', 'zstd_dep'], required: false)
endif
xxhash_dep = dependency('xxhash', fallback: ['xxhash', 'xxhash_dep'], required: false)
argparse_dep = dependency('argparse', required: false)

deps = [lz4_dep, zstd_dep, xxhash_dep, argparse_dep]
if not std_expected_ok
  tl_expected_dep = dependency('tl-expected', fallback: ['tl-expected', 'tl_expected_dep'], required: false)
  deps += tl_expected_dep
endif

inc = include_directories('include')
src_inc = include_directories('src')

bwm_core_sources = files(
  'src/app/engine.cpp',
  'src/app/runtime_runner.cpp',
  'src/app/math_utils.cpp',
  'src/medium/medium.cpp',
  'src/scheduler/scheduler.cpp',
  'src/codec/runtime_codec.cpp',
  'src/codec/codec.cpp',
  'src/generator/low_entropy_generator.cpp',
  'src/sink/hash_sink.cpp',
  'src/bench/protocol.cpp',
)

bwm_core = static_library(
  'bwm_core',
  bwm_core_sources,
  include_directories: [inc, src_inc],
  dependencies: deps,
  install: false,
)

bwm_exe = executable(
  'bwm',
  files('src/main.cpp'),
  include_directories: [inc, src_inc],
  dependencies: deps,
  link_with: bwm_core,
  install: false,
)

if get_option('enable_tests')
  subdir('tests')
endif
