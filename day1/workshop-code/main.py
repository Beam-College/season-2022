#  Copyright 2022 Israel Herraiz
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.

import argparse

from typing import List, Tuple

import apache_beam as beam
from apache_beam import PCollection
from apache_beam.options.pipeline_options import PipelineOptions


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-location", required=True)
    parser.add_argument("--output-location", required=True)
    parser.add_argument("--num-of-words", default=1000, required=False)

    my_args, beam_args = parser.parse_known_args()
    run_pipeline(my_args, beam_args)


def sanitize_word(word: str) -> str:
    word = word.lower()
    word = word.replace(",", "").replace(".", "")
    return word


def prettify(tl: List[Tuple[str, int]]) -> str:
    pretty_str = ""
    for t in tl:
        pretty_str += f"{t[0]},{t[1]}\n"
    return pretty_str


def run_pipeline(custom_args, beam_args):
    opts = PipelineOptions(beam_args)

    input_location = custom_args.input_location
    output_location = custom_args.output_location
    num_words = custom_args.num_of_words

    with beam.pipeline.Pipeline(options=opts) as p:
        # Reading the data
        lines: PCollection[str] = p | "Reading input data" >> beam.io.ReadFromText(file_pattern=input_location)

        # "En un lugar de la Mancha", "cuyo nombre...", "another sentence", ...
        words: PCollection[str] = lines | "Split words" >> beam.FlatMap(lambda line: line.split())

        # "En", "un", "lugar", ..., "en"
        sanitized: PCollection[str] = words | "Sanitize words" >> beam.Map(sanitize_word)
        # "en", "un", "lugar", ..., "en"

        counted: PCollection[Tuple[str, int]] = sanitized | "Count" >> beam.combiners.Count.PerElement()

        ranked: PCollection[List[Tuple[str, int]]] = counted | "Rank" >> beam.combiners.Top.Of(num_words,
                                                                                               key=lambda t: t[1])

        prettied: PCollection[str] = ranked | "Pretty print" >> beam.Map(prettify)
        prettied | "Write output" >> beam.io.WriteToText(output_location)


if __name__ == '__main__':
    # Run this script with this command (in the shell):
    # python main.py --runner=DirectRunner --input-location=./data/el_quijote.txt --output-location=out/words.csv
    main()
