import contextlib

import click
import fastavro
import ujson as json

from tqdm import tqdm

from .utils import get_codecs, ignoring


try:
    import fastavro_codecs
    fastavro_codecs.install_all()
except ImportError:
    pass


iter_records = fastavro.iter_avro


def write_records(fp, records, fail_safe=False, **kwargs):
    writer = fastavro._writer.Writer(fp, **kwargs)
    writer.close = writer.flush
    with contextlib.closing(writer):
        write = writer.write
        for record in records:
            if fail_safe:
                try:
                    write(record)
                except Exception as e:
                    click.echo(("Failed to write record: %s\n%r" % (e, record)), err=True)
            else:
                write(record)


SYNC_INTERVAL = 16 * 1024


def show_progress(it, unit=' records', mininterval=.5, **kwargs):
    return tqdm(it, unit=unit, mininterval=mininterval, **kwargs)


@click.group()
def cli():
    """Avro CLI"""


@cli.command()
def codecs():
    """List available codecs."""
    click.echo("\n".join(sorted(get_codecs())))


@cli.command()
@click.argument('input', nargs=-1, required=True,
                type=click.File(mode='rb', lazy=True))
@click.argument('output', type=click.File(mode='wb'))
def concat(input, output):
    """Concatenates avro files without re-compressing."""
    # TODO: Check compatibility of schema.
    # TODO: Use first codec or override via option
    # TODO: Reach each file and write one.


@cli.command()
@click.argument('input', type=click.File(mode='rt', lazy=True))
@click.argument('output', type=click.File(mode='wb', lazy=True))
@click.option('-s', '--schema', type=click.File(mode='rt', lazy=True))
@click.option('-c', '--codec', metavar='CODEC', default='snappy',
              type=click.Choice(get_codecs()), show_default=True,
              help="Codec to use")
@click.option('--sync-interval', type=int, default=SYNC_INTERVAL)
@click.option('-m', '--metadata', metavar='KEY=VALUE', multiple=True,
              help="Metadata values")
@click.option('--json-value/--no-json-value')
@click.option('--progress/--no-progress')
@click.option('--fail-safe/--no-fail-safe')
@ignoring(BrokenPipeError)
def fromjson(input, output, schema, codec, sync_interval, metadata, json_value,
             progress, fail_safe):
    """Reads JSON records and writes an Avro data file."""
    # TODO: infer schema.
    try:
        # TODO: Use a type class to load json file.
        schema = json.loads(schema.read())
    except Exception as e:
        raise click.UsageError("Could not load schema file: %s" % e)

    metadata = _metadata_from_args(metadata)
    records = _iter_json_values(input, wrap=json_value)
    if progress:
        records = show_progress(records)
    with output:
        write_records(output,
                      schema=schema,
                      records=records,
                      codec=codec,
                      sync_interval=sync_interval,
                      metadata=metadata,
                      fail_safe=fail_safe)


@cli.command()
@click.argument('input', nargs=-1, required=True,
                type=click.File(mode='rb', lazy=True))
@click.argument('output', type=click.File(mode='wb'))
@click.option('-c', '--codec', metavar='CODEC', default='snappy',
              type=click.Choice(get_codecs()), show_default=True,
              help="Codec to use")
@click.option('--sync-interval', type=int, default=SYNC_INTERVAL)
@click.option('-m', '--metadata', metavar='KEY=VALUE', multiple=True,
              help="Metadata values")
@ignoring(BrokenPipeError)
def fromtext(input, codec, sync_interval, metadata, output):
    """Imports a text file into an avro data file (default to stdout)."""
    # TODO: Add validator option.
    schema = {"type": "bytes"}
    metadata = _metadata_from_args(metadata)
    records = _iter_lines(*input)
    with output:
        write_records(output,
                      schema=schema,
                      records=records,
                      codec=codec,
                      sync_interval=sync_interval,
                      metadata=metadata)


def _iter_lines(*input):
    for fp in input:
        with fp:
            for line in fp:
                yield line


def _iter_json_values(*input, wrap=True):
    for line in _iter_lines(*input):
        if line.strip():
            obj = json.loads(line)
            if wrap:
                obj = {'value': obj}
            yield obj


def _metadata_from_args(args):
    metadata_dict = {}
    for meta in args:
        key, _, val = meta.partition('=')
        if not val:
            raise click.UsageError("Invalid metadata option: %s" % meta)
        metadata_dict[key] = val
    return metadata_dict


@cli.command()
@click.argument('file', type=click.File(mode='rb', lazy=True))
def getmeta(file):
    """Prints out the metadata of an Avro data file."""
    with file:
        av = iter_records(file)
        # TODO: Remove 'avro.*' keys.
        click.echo(json.dumps(av.metadata, sort_keys=True, indent=2))


@cli.command()
@click.argument('file', type=click.File(mode='rb', lazy=True))
def getschema(file):
    """Prints out schema of an Avro data file."""
    with file:
        av = iter_records(file)
        schema = json.loads(av.metadata['avro.schema'])
        click.echo(json.dumps(schema, sort_keys=True, indent=2))


@cli.command()
@click.argument('input', type=click.File(mode='rb', lazy=True))
@click.argument('output', type=click.File(mode='wb'))
@click.option('-c', '--codec', metavar='CODEC',
              type=click.Choice(get_codecs()))
@click.option('--sync-interval', type=int, default=SYNC_INTERVAL)
@click.option('-m', '--metadata', metavar='KEY=VALUE', multiple=True,
              help="Metadata values")
@ignoring(BrokenPipeError)
def recodec(input, output, codec, sync_interval, metadata):
    """Alters the codec of a data file."""
    av = iter_records(input)
    # Use same code if not eplicit given.
    codec = codec or av.codec
    records = iter(av)
    metadata_out = dict(av.metadata)
    metadata_out.update(_metadata_from_args(metadata))
    with output:
        write_records(output,
                      schema=av.schema,
                      records=records,
                      codec=codec,
                      sync_interval=sync_interval,
                      metadata=metadata_out)


@cli.command()
@click.argument('input', nargs=-1, required=True,
                type=click.File(mode='rb', lazy=True))
@click.argument('output', type=click.File(mode='wt'))
@click.option('--progress/--no-progress')
@ignoring(BrokenPipeError)
def tojson(input, output, progress):
    """Dumps an Avro data file as JSON, one record per line."""
    with output:
        dumps = json.dumps
        write = output.write
        for fp in input:
            with fp:
                records = iter_records(fp)
                if progress:
                    records = show_progress(records)
                for record in records:
                    write(dumps(record))
                    write("\n")


@cli.command()
@click.argument('input', nargs=-1, required=True,
                type=click.File(mode='rb', lazy=True))
@click.argument('output', type=click.File(mode='wb'))
@click.option('--progress/--no-progress')
@ignoring(BrokenPipeError)
def totext(input, output, progress):
    """Converts one Avro data file to a text file."""
    if hasattr(output, 'open'):
        out = output.open()
    else:
        out = output
    with out:
        for infile in input:
            with infile.open() as fp:
                records = iter_records(fp)
                if progress:
                    records = show_progress(records)
                for bytes in records:
                    out.write(bytes)


@cli.command()
@click.argument('input', type=click.File(mode='rb', lazy=True))
@ignoring(BrokenPipeError)
def readbench(input):
    """Dumps an Avro data file as JSON, one record per line."""
    with input:
        for _ in show_progress(iter_records(input), desc='Reading'):
            pass


if __name__ == "__main__":
    cli()
