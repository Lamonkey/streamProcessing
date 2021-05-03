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
        self.output_fn = output_fn
        self.buffer = []
        self.num_out = 0

    def format_updateStateByKey(self,):
        processed_lines = []
        lines = open(self.template_fn, 'r').readlines()

        for i, line in enumerate(lines):
            # aggregate
            if self.num_out == 1:
                processed_lines.append(" " * 4 +line)

            elif "update_value" in line and "sum" in line and "running_value" in line:
                for j in range(self.num_out):
                    update_line = (
                        line.replace("update_value", "update_value[{}]".format(j))
                        .replace("[x", "[x[{}]".format(j))
                        .replace("running_value", "running_value[{}]".format(j))
                    )
                    processed_lines.append(" " * 4 +update_line)
            # init update_value
            elif "0" in line and ("update_value" in line or "running_value" in line) :
                init_value = "["
                for j in range(self.num_out):
                    if j != self.num_out - 1:
                        init_value += "0, "
                    else:
                        init_value += "0]"
                line = line.replace("0", init_value)
                processed_lines.append(" " * 4 +line)
            else:
                processed_lines.append(" " * 4 + line)

        return processed_lines
    def insert_updateStateByKey(self):
        '''Inserting the template for updateStatesByKey'''
        update_state_func = self.format_updateStateByKey()
        self.buffer = self.buffer[:1] + update_state_func + self.buffer[1:]

    def input_to_buffer(self, lines):
        for line in lines:
            if "batch_pipeline" in line:
                line = line.replace("batch", "stream")
            # identify the format of data pipeline
            if "reduceByKey" in line:
                out = line.split("(")[-1].strip(")")
                self.num_out = len(out.split(","))
            elif "sortBy" in line:
                line = line.replace(".", ".transform(lambda rdd: rdd.").replace("\n", ")\n")
                print(line)
            elif "take" in line:
                line = line.replace("take", "pprint")
            self.buffer.append(line)

    def buffer_to_output(self,):
        res_file = open(self.output_fn, "w")
        for i, line in enumerate(self.buffer):
            if "reduceByKey" in line:
                res_file.write(line)
                res_file.write("    " * 2 + ".updateStateByKey(updateFunc)\n")

            else:
                res_file.write(line)
        res_file.close()

    def refractor(self):
        f = open(self.input_fn, "r")
        lines = f.readlines()
        f.close()

        self.input_to_buffer(lines)
        self.insert_updateStateByKey()
        self.buffer_to_output()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='auto refractor batch pipeline to stream pipeline')
    parser.add_argument('--f_in', type=str, default="./sample/wc/batch_pipeline.txt", help='input filename')
    parser.add_argument('--f_gt', type=str, default="./sample/wc/gt_stream_pipeline.txt", help='output filename')
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

    gt_lines = open(args.f_gt).readlines()
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