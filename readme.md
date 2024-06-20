# IP2Location to MaxMind MMDB Converter

This project provides a Python script to convert IP2Location LITE CSV database to MaxMind MMDB (GeoIP2) format. It supports both country-level and city-level databases.

## Features

- Converts IP2Location LITE CSV to MaxMind MMDB format
- Supports both country and city level databases
- Efficient processing with progress bars
- Generates MMDB files compatible with MaxMind GeoIP2 APIs

## Requirements

- Python 3.5+
- tqdm library (for progress bars)

## Installation

1. Clone this repository:
   ```
   git clone https://github.com/ding113/convert-ip2location-to-mmdb.git
   cd convert-ip2location-to-mmdb
   ```

2. Install the required dependencies:
   ```
   pip install tqdm
   ```

## Usage

1. Download the IP2Location LITE CSV database (DB1 for country-level or DB11 for city-level) from [IP2Location LITE](https://lite.ip2location.com/ip2location-lite).

2. Run the conversion script:
   ```
   python convert.py <path_to_IP2Location_CSV_file>
   ```

3. The script will generate an MMDB file in the same directory as the input CSV file.

## Example

```
python convert.py IP2LOCATION-LITE-DB1.CSV
```

This will generate `IP2LOCATION-LITE-DB1.CSV.MMDB` in the same directory.

## Output

The generated MMDB file can be used with any MaxMind API that supports the GeoLite2 format.

## Notes

- This script is based on the work by [antonvlad999](https://github.com/antonvlad999/convert-ip2location-geolite2) and has been modified for improved performance and usability.
- The conversion process may take some time depending on the size of the input database.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Disclaimer

IP2Location and MaxMind are trademarks of their respective owners. This project is not affiliated with or endorsed by IP2Location or MaxMind.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## Support

If you encounter any problems or have any questions, please open an issue in this repository.
