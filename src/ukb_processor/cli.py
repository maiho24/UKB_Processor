import typer
from pathlib import Path
from rich.console import Console
from rich import print as rprint
from typing import List, Optional
from . import converter, extractor

app = typer.Typer(
    help="UK Biobank data processing tools",
    context_settings={"help_option_names": ["--help", "-h"]}
)
console = Console()

@app.command()
def convert(
    input_file: Path = typer.Argument(..., help="Input CSV file"),
    output_file: Path = typer.Argument(..., help="Output Parquet file"),
    compression: str = typer.Option("zstd", help="Compression algorithm"),
    chunk_size: int = typer.Option(50000, help="Chunk size for processing")
):
    """Convert UKB CSV file to Parquet format."""
    try:
        converter.csv_to_parquet(input_file, output_file, compression, chunk_size)
        rprint(f"[green]✓ Successfully converted {input_file} to {output_file}[/green]")
    except Exception as e:
        rprint(f"[red]Error: {str(e)}[/red]")
        raise typer.Exit(1)

@app.command()
def extract(
    parquet_file: Path = typer.Argument(..., help="Input Parquet file"),
    output_file: Path = typer.Argument(..., help="Output CSV file"),
    field_ids: Optional[List[str]] = typer.Option(None, "--fields", "-f", help="Field IDs to extract"),
    field_file: Optional[Path] = typer.Option(None, "--file", help="Text file with field IDs"),
    remove_empty: bool = typer.Option(False, "--remove-empty", "-r", help="Remove rows where all extracted fields are empty"),
    instance: Optional[str] = typer.Option(None, "--instance", "-i", help="Extract specific instance only (e.g., '1.0', '2.0')")
):
    """
    Extract specified fields from Parquet file.
    
    Fields can be specified either by --fields or --file or both.
    When both are provided, fields from both sources will be combined.
    """
    if not field_ids and not field_file:
        rprint("[red]Error: Must provide either --fields or --file[/red]")
        raise typer.Exit(1)
        
    try:
        processed_fields = extractor.extract_fields(
            parquet_file,
            output_file,
            field_ids=field_ids,
            field_file=field_file,
            remove_empty=remove_empty,
            instance=instance
        )
        
        rprint(f"[green]✓ Successfully extracted fields to {output_file}[/green]")
        
        # Show summary of processed fields in a single line
        if field_ids and field_file:
            rprint("\n[yellow]Note: Combined fields from both command line and file[/yellow]")
            
        fields_str = " | ".join(sorted(processed_fields))
        rprint(f"\n[bold]Processed fields:[/bold] {fields_str}")
            
        if field_ids and field_file:
            # Show which fields came from where if there's overlap
            cmd_fields = set(field_ids)
            file_fields = set(extractor.read_field_ids(field_file))
            overlap = cmd_fields & file_fields
            
            if overlap:
                overlap_str = " | ".join(sorted(overlap))
                rprint(f"\n[yellow]Duplicate fields:[/yellow] {overlap_str}")
                    
    except Exception as e:
        rprint(f"[red]Error: {str(e)}[/red]")
        raise typer.Exit(1)

if __name__ == "__main__":
    app()