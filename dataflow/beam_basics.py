import apache_beam as beam


def parse_csv(row):
    fields = row.split(",")

    return {
        "order_id": int(fields[0]),
        "customer": fields[1],
        "product": fields[2],
        "quantity": int(fields[3]),
        "amount": float(fields[4]),
        "order_date": fields[5]
    }


with beam.Pipeline() as pipeline:

    sales = (
        pipeline
        | "Read sales CSV" >> beam.io.ReadFromText(
            "data/sales.csv",
            skip_header_lines=1
        )
        | "Parse CSV rows" >> beam.Map(parse_csv)
    )

class CalculateTotalAmount(beam.DoFn):

    def process(self, row):
        row["total_amount"] = row["quantity"] * row["amount"]
        yield row


with beam.Pipeline() as pipeline:

    sales = (
        pipeline
        | "Read sales CSV" >> beam.io.ReadFromText(
            "data/sales.csv",
            skip_header_lines=1
        )
        | "Parse CSV rows" >> beam.Map(parse_csv)
    )

    sales_with_total = (
        sales
        | "Calculate total amount" >> beam.ParDo(CalculateTotalAmount())
        | "Print sales with total" >> beam.Map(print)
    )