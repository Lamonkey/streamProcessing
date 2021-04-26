import pdb as bp
import argparse
import os

class PipelineRefractor:
    def __init__(self, input_fn, output_fn, template_fn):
        """[summary]

        Args:
            input_fn ([str]): [filename of a .py file for batch processing pipeline]
            output_fn ([str]): [filename of a .py file for stream processing pipeline ]
        """
        self.input_fn = input_fn
        self.template_fn = template_fn
        self.res_file = open(output_fn, "w")

    def insert_template(self):
        for temp_line in open(self.template_fn).readlines():
            if len(temp_line) != 1:
                res = "    " + temp_line
            else:
                res = temp_line
            self.res_file.write(res)
        self.res_file.write("\n" * 2)

    def refractor(self):
        f = open(self.input_fn, "r")
        lines = f.readlines()

        for i, line in enumerate(lines):
            if line.startswith("def"):
                self.res_file.write(line.replace("batch", "stream"))
                self.insert_template()
            elif i == len(lines) - 2:
                self.res_file.write("    " * 2 + ".updateStateByKey(updateFunc)\n")
                self.res_file.write(line)
            else:
                self.res_file.write(line)

        f.close()
        self.res_file.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='auto refractor batch pipeline to stream pipeline')
    parser.add_argument('--f_in', type=str, default="./sample/batch_pipeline.txt", help='input filename')
    parser.add_argument('--f_out', type=str, default="./gen_stream_pipeline.py", help='output filename')
    args = parser.parse_args()

    # auto linting with "black" (black package is required, using pip install)
    os.system("black {}".format(args.f_in))

    pipeline = PipelineRefractor(
        input_fn=args.f_in,
        output_fn=args.f_out,
        template_fn="./sample/template.txt",
    )
    pipeline.refractor()

    gt_lines = open("./sample/gt_stream_pipeline.txt").readlines()
    res_lines = open("./gen_stream_pipeline.py").readlines()

    length = 80
    for gt, res in zip(gt_lines, res_lines):
        if gt == res:
            gt = gt.strip("\n")
            if len(gt) < length:
                gt = gt + " " * (length - len(gt))
            print("{} \t\t ------ pass".format(gt))
        else:
            bp.set_trace()
