package(default_visibility = ["//visibility:public"])

java_library(
  name = "jelectrumlib",
  srcs = glob(["src/**/*.java", "src/*.java"]),
  deps = [
    "@duckutil//:duckutil_lib",
    "@snowblossom//lib",
    "@maven//:com_google_guava_guava",
    "@maven//:org_bitcoinj_bitcoinj_core",
    "@maven//:com_google_protobuf_protobuf_java",
    "@maven//:junit_junit",
    "@maven//:org_rocksdb_rocksdbjni",
    "@maven//:org_json_json",
    "@maven//:commons_codec_commons_codec",
    
  ],
)

java_binary(
  name = "Jelectrum",
  main_class = "jelectrum.Jelectrum",
  runtime_deps = [
    ":jelectrumlib",
  ],
)

java_test(
    name = "script_hash_test",
    srcs = ["test/ScriptHashTest.java"],
    test_class = "ScriptHashTest",
    size="small",
    deps = [
      ":jelectrumlib",
      "@maven//:com_google_protobuf_protobuf_java",
      "@maven//:commons_codec_commons_codec",
      "@maven//:org_bitcoinj_bitcoinj_core",
      "@maven//:org_json_json",
    ],
)

