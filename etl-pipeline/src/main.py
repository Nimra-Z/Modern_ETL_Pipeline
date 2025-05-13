import argparse
from pathlib import Path
from pipeline import ETLPipeline
from loguru import logger

def main():
    """Main entry point for the ETL pipeline."""
    parser = argparse.ArgumentParser(description='Run the ETL pipeline')
    parser.add_argument(
        '--config',
        type=str,
        default='config/config.yaml',
        help='Path to the configuration file'
    )
    args = parser.parse_args()
    
    # Ensure config file exists
    config_path = Path(args.config)
    if not config_path.exists():
        logger.error(f"Configuration file not found: {config_path}")
        return False
    
    # Create logs directory if it doesn't exist
    Path('logs').mkdir(exist_ok=True)
    
    # Run the pipeline
    pipeline = ETLPipeline(str(config_path))
    success = pipeline.run()
    
    return success

if __name__ == '__main__':
    success = main()
    exit(0 if success else 1) 