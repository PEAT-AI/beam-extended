# beam-extended package

Provide extensions to existing SDKs (mainly Python)

It currently contains:
- mongoDB IO Connector

## Installation

To install the package, please use the `pip` installation as follows:

    pip install beam-extended
    
**Note:** currently, due to Apache Beam constraints, beam-extended is only available for **Python 3.6**.

## Example Usage

Here is a short example of using the package.

    from beam_extended.io.mongodbio import ReadFromMongo, WriteToMongo
    ...
    with beam.Pipeline(options=options) as pipeline:
        ( p | 'read' >> ReadFromMongo('myUrl1', 'myDb1', 'myColl1', query={}, projection=['_id'])
            | 'transform' >> beam.Map(transform)
            | 'save' >> WriteToMongo('myUrl2', 'myDb2', 'myColl2'))


## Publishing

Run the `pypi.sh` script.

## Credits

Based on:

https://gist.github.com/dlebech/e9d6ba266014db8783dbbeb362593020 by David Volquartz Lebech

and

https://gist.github.com/sandboxws/08b9c5e373b94056733f8a662d9a2fa2 by Ahmed El.Hussaini