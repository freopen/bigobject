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
        LIBCLANG_PATH = "${pkgs.libclang.lib}/lib";
        packages = (with pkgs; [
          nil
          nixfmt
          (fenix.packages.x86_64-linux.complete.withComponents [
            "cargo"
            "clippy"
            "rust-src"
            "rustc"
            "rustfmt"
          ])
        ]);
      };
    };
}
