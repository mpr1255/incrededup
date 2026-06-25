# Third-party notices

## Rensa

The core R-MinHash implementation in `src/minhash/` is derived from Rensa:

https://github.com/beowolx/rensa

The in-memory LSH banding and candidate lookup used by `src/lsh/mod.rs` is also
adapted from Rensa's `RMinHashLSH` design. `incrededup` removes the PyO3/Python
surface, adapts the code to Rust-native APIs and UUID document IDs, and wraps
the algorithm in a disk-backed `redb` sidecar pipeline and CLI.

Rensa was checked against upstream commit
`5d830318a7e598db94a2e6b2a3491c913433c7d3`. The local implementation most
closely matches the older Rensa source shape around commit
`f1ebafb1e7cbcfe4294fed384c195c97dbc30a44`; Rensa has evolved since then.

Rensa copyright and license notice:

```text
MIT License

Copyright (c) 2024 beowulf

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```
