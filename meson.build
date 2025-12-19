project('dolphindb', 'cpp', 'c',
  version : '3.2.5',
  default_options : ['warning_level=3', 'cpp_std=c++23,c++20,c++17,c++14,c++11'])

subdir('src')

uuid = dependency('uuid')
ssl = dependency('OpenSSL')

library('dolphindb',
        src,
        include_directories : inc,
        dependencies : [uuid, ssl],
        cpp_args : '-DUSE_OPENSSL',
        install : true)
