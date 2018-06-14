import argparse

'''
Just parsing args
'''

parser = argparse.ArgumentParser()
parser.add_argument('-ip', '--ip_file', type=str, help='input file')
parser.add_argument('-op', '--op_path', type=str, help='output path')
parser.add_argument('-ed', '--edit_dist', type=str, help='edit distance', default=1)
parser.add_argument('-ki', '--key_ing', type=str, help='key ingredients', default='Chilies')

args = parser.parse_args()
