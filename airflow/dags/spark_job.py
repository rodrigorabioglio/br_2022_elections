from spark_operators import prepare_data_and_write_to_bigquery

from const import execution_dict
import argparse

def main(args):

    bucket_name = args['bucket_name']
    ds = args['ds']
    
    state = execution_dict[ds]

    prepare_data_and_write_to_bigquery(bucket_name,state)


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument('-b','--bucket_name', required=True)
    parser.add_argument('-ds','--ds', required=True)
    args = vars(parser.parse_args())

    main(args)
