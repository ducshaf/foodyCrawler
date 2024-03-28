import multiprocessing as mp
from multiprocessing.pool import ThreadPool
from tqdm import tqdm
from datetime import datetime

class CommonUtils:

    @staticmethod
    def get_date_time(format_str: str = '%Y-%m-%d %H:%M:%S') -> str:
        return datetime.now().strftime(format_str)

    @staticmethod
    def process_list(inputs: list, func: callable, desc: str ='Processing list', method: str = 'multi') -> list:
        outputs: list = []
        if inputs:
            if method == 'multi':
                with mp.Pool(4) as p:
                    outputs = p.starmap(func, inputs)
            elif method == 'single':
                for input in tqdm(inputs, desc=desc):
                    outputs.append(func(input))
        return outputs

    @staticmethod
    def write_output(outputs: list, output_path: str):
        with open(output_path, 'w') as f:
            for output in outputs:
                f.write(output + '\n')