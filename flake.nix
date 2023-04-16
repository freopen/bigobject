{
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    fenix = {
      url = "github:nix-community/fenix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = { self, nixpkgs, fenix }:
    let
      pkgs = nixpkgs.legacyPackages.x86_64-linux.extend fenix.overlays.default;
    in {
      devShells.x86_64-linux.default = pkgs.mkShell {
        src = ./.;
        __noChroot = true;
        LIBCLANG_PATH = "${pkgs.libclang.lib}/lib";
        RUSTFLAGS = "-Z macro-backtrace";
        packages = (with pkgs; [
          nil
          nixfmt
          clang
          (fenix.packages.x86_64-linux.complete.withComponents [
            "cargo"
            "clippy"
            "rust-src"
            "rustc"
            "rustfmt"
          ])
        ]);
        buildPhase = ''
          cp -R $src/* .
          export CARGO_HOME=$(mktemp -d cargo-home.XXX)
          cargo test
          cargo clippy
          touch $out
        '';
      };
    };
}
