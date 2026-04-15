from src.pipeline import PipelineETL

def main():
    input_path = "./data/movies.csv"
    
    pipeline_etl = PipelineETL(input_path)

    pipeline_etl.run()

if __name__ == "__main__":
    main()