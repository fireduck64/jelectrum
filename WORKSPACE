load("@bazel_tools//tools/build_defs/repo:git.bzl", "git_repository")
load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

git_repository(
    name = "rules_jvm_external",
    remote = "https://github.com/bazelbuild/rules_jvm_external",
    commit = "9aec21a7eff032dfbdcf728bb608fe1a02c54124",
    shallow_since = "1577467222 -0500"
)

load("@rules_jvm_external//:defs.bzl", "maven_install")

git_repository(
  name = "build_stack_rules_proto",
  remote = "https://github.com/fireduck64/rules_proto",
  commit = "8ab7bf0c7c992c893813f7151af4794ec5dd3e3f",
  shallow_since = "1579204983 -0800"
)

load("@build_stack_rules_proto//:deps.bzl", "io_grpc_grpc_java")
load("@build_stack_rules_proto//java:deps.bzl", "java_proto_compile")

io_grpc_grpc_java()
java_proto_compile()

load("@io_grpc_grpc_java//:repositories.bzl", "grpc_java_repositories")

grpc_java_repositories()

load("@build_stack_rules_proto//java:deps.bzl", "java_grpc_library")

java_grpc_library()


git_repository(
  name = "duckutil",
  remote = "https://github.com/fireduck64/duckutil",
  commit = "0dd4f3aff5bab1f9ce9f4ac326c120c47b7e71e0",
  shallow_since = "1576867546 -0800",
)

git_repository(
  name = "snowblossom",
  remote = "https://github.com/snowblossomcoin/snowblossom",
  commit = "c634cd4b21f33588e2df08eb80c6987769b1ad04",
  shallow_since = "1576867546 -0800",
)



maven_install(
    artifacts = [
        "com.google.protobuf:protobuf-java:3.5.1",
        "org.rocksdb:rocksdbjni:5.14.2",
        "junit:junit:4.12",
        "com.google.guava:guava:28.1-jre",
        "org.bitcoinj:bitcoinj-core:0.15.8",
        "com.thetransactioncompany:jsonrpc2-server:1.11",
        "org.json:json:20200518",
        "commons-codec:commons-codec:1.14",
        "org.apache.commons:commons-math3:3.6.1",
        "org.bouncycastle:bcprov-jdk15on:1.65",
        "org.bouncycastle:bcpkix-jdk15on:1.65",
        "io.netty:netty-tcnative-boringssl-static:2.0.25.Final",
        "org.zeromq:jeromq:0.5.2",
    ],
    repositories = [
        "https://repo1.maven.org/maven2",
        "https://maven.google.com",
    ],
    #maven_install_json = "//:maven_install.json",
)
# After changes run:
# bazel run @unpinned_maven//:pin

load("@maven//:defs.bzl", "pinned_maven_install")
pinned_maven_install()

