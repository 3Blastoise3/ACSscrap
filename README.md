# ACS Data Scraper

A browser-based tool to extract data from American Community Survey (ACS) Excel files. Runs entirely in your browser with no server required.

## Usage

1. Open `index.html` in a web browser
2. Upload an ACS Excel file (drag-and-drop or click to select)
3. Select the sheet containing your data
4. Configure geography settings (States, Metro Areas, or Counties)
5. Set the column pattern to match your file's structure
6. Add row specifications for the data you want to extract
7. Click "Extract Data" to view results
8. Copy to clipboard (TSV format) or export as CSV

## Configuration Options

### Geography Type
- **States**: Automatically uses all 50 US states + DC
- **Metro Areas**: Paste metro area names, one per line
- **Counties**: Paste county names, one per line

### Column Pattern
- **Columns per Geography**: Number of columns each geography occupies (e.g., 6)
- **Target Column**: Which column to extract from each geography block (1-based)
- **Starting Column**: First column of data (e.g., B)

### Row Specifications
- Single row: `4`
- Multiple rows (summed): `4,5,6`
- Range (summed): `11-14`
- Add labels to identify each extraction

### Percentage Calculation
Enable to calculate percentages from extracted rows. Specify numerator and denominator by their labels.

## Features

- Flexible column pattern configuration
- Row summing with range syntax (e.g., "11-14")
- Fuzzy matching for geography names
- TSV output for direct Excel paste
- CSV export option
- No installation required

## Browser Compatibility

Works in modern browsers (Chrome, Firefox, Safari, Edge). Requires JavaScript enabled.

## Dependencies

Uses [SheetJS](https://sheetjs.com/) via CDN for Excel file parsing.
