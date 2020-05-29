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
  name = "duckutil",
  remote = "https://github.com/fireduck64/duckutil",
  commit = "0dd4f3aff5bab1f9ce9f4ac326c120c47b7e71e0",
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

