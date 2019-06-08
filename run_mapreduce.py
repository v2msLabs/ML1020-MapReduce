import multiprocessing
import string
import sys
import time
from simple_mapreduce import SimpleMapReduce

default_input_dir = "../texts"
# set default directory for the temporary map files
# set default directory for the output files
default_output_dir = "../result"

def file_to_words(filename):
    """Read a file and return a sequence of (word, occurances) values.
    """
    STOP_WORDS = {
        'a', 'an', 'and', 'are', 'as', 'be', 'by', 'for', 'if', 'in',
        'is', 'it', 'of', 'or', 'py', 'rst', 'that', 'the', 'to', 'with'
    }
    TR = str.maketrans(string.punctuation, ' ' * len(string.punctuation))

   # print(multiprocessing.current_process().name, 'reading', filename)
    output = []

    with open(filename, 'r',encoding="utf-8") as f:
        try:
         for line in f:
            if line.lstrip().startswith('..'):  # Skip rst comment lines
                continue
            line = line.translate(TR)  # Strip punctuation
            for word in line.split():
                word = word.lower()
                if word.isalpha() and word not in STOP_WORDS:
                    output.append((word, 1))
        except Exception as e:
         print('Exception')
    return output


def count_words(item):
    """Convert the partitioned data for a word to a
    tuple containing the word and the number of occurances.
    """
    word, occurances = item
    return (word, sum(occurances))


if __name__ == '__main__':
    import operator
    import glob

    print(
        "Syntax: run_mapreduce [input_dir] [output_dir]")

    if (len(sys.argv) == 1):
        input_dir = default_input_dir
        output_dir = default_output_dir
    else:
        input_dir = sys.argv[1]
        output_dir = sys.argv[2]

    print('Input directory: ', default_input_dir)
    print('Output directory: ', default_output_dir)
    input_files = glob.glob(input_dir+'\*.*')
    start = time.time()
    mapper = SimpleMapReduce(file_to_words, count_words)
    word_counts = mapper(input_files)
#    word_counts.sort(key=operator.itemgetter(1))
#    word_counts.reverse()

    with open(output_dir+'\output.txt', 'w',encoding="utf-8") as f:
        f.writelines('%s %s\n' % tu for tu in word_counts)
    print("-- Finished. Elapsed time (sec):", time.time() - start)
