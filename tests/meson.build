test_common_inc = include_directories('..', '../include', '../src')

unit_codec = executable(
  'bwm_codec_roundtrip_tests',
  files('unit/codec_roundtrip_tests.cpp'),
  include_directories: test_common_inc,
  dependencies: deps,
  link_with: bwm_core,
  install: false,
)
test('bwm_codec_roundtrip_tests', unit_codec)

unit_gen = executable(
  'bwm_generator_determinism_tests',
  files('unit/generator_determinism_tests.cpp'),
  include_directories: test_common_inc,
  dependencies: deps,
  link_with: bwm_core,
  install: false,
)
test('bwm_generator_determinism_tests', unit_gen)

unit_protocol = executable(
  'bwm_protocol_metrics_tests',
  files('unit/protocol_metrics_tests.cpp'),
  include_directories: test_common_inc,
  dependencies: deps,
  link_with: bwm_core,
  install: false,
)
test('bwm_protocol_metrics_tests', unit_protocol)

unit_medium = executable(
  'bwm_medium_factory_tests',
  files('unit/medium_factory_tests.cpp'),
  include_directories: test_common_inc,
  dependencies: deps,
  link_with: bwm_core,
  install: false,
)
test('bwm_medium_factory_tests', unit_medium)

unit_scheduler = executable(
  'bwm_scheduler_factory_tests',
  files('unit/scheduler_factory_tests.cpp'),
  include_directories: test_common_inc,
  dependencies: deps,
  link_with: bwm_core,
  install: false,
)
test('bwm_scheduler_factory_tests', unit_scheduler)

unit_protocol_runner = executable(
  'bwm_protocol_runner_tests',
  files('unit/protocol_runner_tests.cpp'),
  include_directories: test_common_inc,
  dependencies: deps,
  link_with: bwm_core,
  install: false,
)
test('bwm_protocol_runner_tests', unit_protocol_runner)

sh_prog = find_program('sh')

test(
  'bwm_disk_e2e_validation',
  sh_prog,
  args: [meson.current_source_dir() + '/integration/disk_e2e_validation.sh', bwm_exe.full_path()],
  workdir: meson.project_build_root(),
  timeout: 180,
)

test(
  'bwm_tcp_loopback_e2e_validation',
  sh_prog,
  args: [meson.current_source_dir() + '/integration/tcp_loopback_e2e_validation.sh', bwm_exe.full_path()],
  workdir: meson.project_build_root(),
  timeout: 180,
)
