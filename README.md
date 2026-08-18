# FormsDB (Rust Implementation)

This repository is the rust implementaion of the original python based FormsDB for AMP-SCZ at https://github.com/AMP-SCZ/ampscz-formsdb

## Features
- Rust implementation of FormsDB
- Better performance compared to the Python implementation
- Easier dependency management with Cargo
  - Helps deploying in CentOS 7 / legacy systems

## Scope
- Implementation not complete
  - includes:
    - Core ingestion from REDCap JSONs and RPMS CSVs
    - Exporting combined CSVs

## Usage

To use the Rust implementation of FormsDB, follow these steps:

1. Clone the repository:

```bash
git clone https://github.com/dheshanm/formsdb-rust.git
cd formsdb-rust
```
2. Build the project using Cargo:

```bash
cargo build --release
```
3. Run the compiled binaries under the `target/release` directory:

```bash
export DB_URI="postgresql://<db_username>:<db_password>@<db_host>:<db_port>/<db_name>"
./target/release/export_combined_csv --help
```