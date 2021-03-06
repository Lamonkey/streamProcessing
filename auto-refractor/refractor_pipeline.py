import pdb as bp
import argparse
import os

class PipelineRefractor:
    def __init__(self, input_fn, output_fn, template_fn):
        self.input_fn = input_fn
        self.template_fn = template_fn
        self.output_fn = output_fn
        self.buffer = []
        self.num_reuse = 0
        self.is_valid = False
        self.ops = []

    def __format_updateStateByKey(self,):
        processed_lines = []
        lines = open(self.template_fn, 'r').readlines()

        for i, line in enumerate(lines):
            if self.num_reuse == 1:
                processed_lines.append(" " * 4 +line)

            elif "update_value" in line and "sum" in line and "running_value" in line:
                for j in range(self.num_reuse):
                    update_line = (
                        line.replace("update_value", "update_value[{}]".format(j))
                        .replace("[x", "[x[{}]".format(j))
                        .replace("+", self.ops[j])
                        .replace("running_value", "running_value[{}]".format(j))
                    )
                    processed_lines.append(" " * 4 +update_line)

            elif "0" in line and ("update_value" in line or "running_value" in line) :
                init_value = "["
                for j in range(self.num_reuse):
                    if j != self.num_reuse - 1:
                        init_value += "0, "
                    else:
                        init_value += "0]"
                line = line.replace("0", init_value)
                processed_lines.append(" " * 4 +line)
            else:
                processed_lines.append(" " * 4 + line)
        return processed_lines

    def __insert_updateStateByKey(self):
        update_state_func = self.__format_updateStateByKey()
        self.buffer = self.buffer[:1] + update_state_func + self.buffer[1:]

    def __input_to_buffer(self, lines):
        for line in lines:
            if "batch_pipeline" in line:
                line = line.replace("batch", "stream")

            if "reduceByKey" in line and "lambda" in line:
                out = line.split("(")[-1].strip("\n").strip(")")
                if "lambda" in out:
                    self.is_valid = False

                self.num_reuse = len(out.split(","))
                for op in out.split(","):
                    op = op.lstrip(" ")
                    l = len(op)
                    self.ops.append(op[l // 2])

            elif "sortBy" in line:
                line = line.replace(".", ".transform(lambda rdd: rdd.").replace("\n", ")\n")

            elif "take" in line:
                line = line.replace("take", "pprint")
            self.buffer.append(line)

    def __buffer_to_output(self, res_file):
        if not self.is_valid:
            return

        for i, line in enumerate(self.buffer):

            if "reduceByKey" in line and "lambda" in line:
                res_file.write(line)
                res_file.write("    " * 2 + ".updateStateByKey(updateFunc)\n")

            else:
                res_file.write(line)
        res_file.close()

    def refractor(self):
        f = open(self.input_fn, "r")
        res_file = open(self.output_fn, "w")
        lines = f.readlines()
        f.close()

        for line in lines:
            if ".map" in line:
                self.is_valid = True

        if not self.is_valid:
            return

        self.__input_to_buffer(lines)
        self.__insert_updateStateByKey()
        self.__buffer_to_output(res_file)
        return self.is_valid


def main():
    parser = argparse.ArgumentParser(description='auto refractor batch pipeline to stream pipeline')
    parser.add_argument('--f_in', type=str, default="./sample/lambda/batch_pipeline.txt", help='input filename')
    parser.add_argument('--f_gt', type=str, default="./sample/lambda/gt_stream_pipeline.txt", help='output filename')
    parser.add_argument('--f_out', type=str, default="./gen_stream_pipeline.py", help='output filename')
    args = parser.parse_args()

    # auto linting with "black" (black package is required, using pip install)
    os.system("black {}".format(args.f_in))

    pipeline = PipelineRefractor(
        input_fn=args.f_in,
        output_fn=args.f_out,
        template_fn="./sample/template.txt",
    )
    valid = pipeline.refractor()

    if not valid:
        return

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



if __name__ == "__main__":
    main()