# Privacy Policy — ColGrep

## Overview

ColGrep is a local-first semantic code search tool. Your source code never leaves your machine.

## Data Processing

- **All indexing and search happens locally** on your machine. No code, embeddings, or search queries are sent to any external server.
- **Search indices** are stored locally in `.colgrep/` within your project directory. They are never uploaded or shared.

## Network Requests

ColGrep makes a **single network request** on first use: downloading the ColBERT embedding model from [HuggingFace Hub](https://huggingface.co/lightonai/LateOn-Code-edge). After the initial download, the model is cached locally and no further network access is required.

No other network requests are made. ColGrep works fully offline after the model is downloaded.

## Telemetry

ColGrep does **not** collect any telemetry, usage data, analytics, or crash reports.

## Source Code Access

ColGrep reads source files in your project directory to build a search index. This index is stored locally and is never transmitted externally. No code leaves your machine.

## Contact

For questions about this privacy policy, contact [contact@lighton.ai](mailto:contact@lighton.ai).
