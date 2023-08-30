# bill_parser

## Description

A brief description of your project goes here.

## Dependencies

- Python 3
- virtualenv

## Setup

1. Clone this repository
    ```bash
    git clone git@github.com:vakordvic/bill_parser.git
    ```
   
2. Navigate to the project directory
    ```bash
    cd bill_parser/fija
    cd bill_parser/movil
    ```
   
3. Create a virtual environment
    ```bash
    virtualenv venv
    ```

4. Activate the virtual environment
    - On macOS and Linux:
        ```bash
        source venv/bin/activate
        ```
    - On Windows:
        ```bash
        .\venv\Scripts\activate
        ```

5. Install required packages
    ```bash
    pip install -r requirements.txt
    ```

## Usage

To run the main program:

```bash
python3 main.py --file <file_path> [--output <output_dir_path>]
