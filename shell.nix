{
  pkgs ? import <nixpkgs> { }
}:

pkgs.mkShell {
  buildInputs = with pkgs; [
    # gnumake
    (python3.withPackages (pp: with pp; [
      # requests
      # nur.repos.milahu.python3.pkgs.aiohttp-chromium
      aiohttp
      aiohttp-retry
      lxml
      tqdm
    ]))
  ];
}
