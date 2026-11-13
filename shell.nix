{ pkgs ? import <nixpkgs> { } }:

# Development shell for hotstuff-2chain.
#
# Regenerate the protobuf code with:
#   protoc --go_out=. consensus.proto
#   protoc --go_out=. mempool.proto
pkgs.mkShell {
  packages = with pkgs; [
    go
    protobuf
    protoc-gen-go
  ];
}
