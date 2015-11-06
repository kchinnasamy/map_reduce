import sys, os

input_folder = sys.argv[1]
output_file = sys.argv[2]

collectable_files = os.listdir(input_folder)

f = open(output_file, "w")

for c_file_name in sorted(collectable_files):
    print c_file_name
    c_file = open(input_folder+"/"+c_file_name)
    f.write(c_file.read())

f.close()