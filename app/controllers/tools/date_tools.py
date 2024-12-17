from datetime import datetime

def api_date_converter(
        hEven: [str, int] = None,
        dEven: [str, int] = None,
        date: datetime = None) -> str:

    # Set date to the current date if not provided
    date_str = date.strftime('%Y-%m-%d') if date else datetime.now().strftime('%Y-%m-%d')

    if hEven and dEven is None:
        hEven = str(hEven).strip()
        return f'{date_str} {hEven[:-4]}:{hEven[-4:-2]}:{hEven[-2:]}'

    elif dEven and hEven is None:
        dEven = str(dEven).strip()
        return f'{dEven[:-4]}-{dEven[-4:-2]}-{dEven[-2:]}'

    elif hEven and dEven:
        dEven = str(dEven).strip()
        hEven = str(hEven).strip()
        return f'{dEven[:-4]}-{dEven[-4:-2]}-{dEven[-2:]} {hEven[:-4]}:{hEven[-4:-2]}:{hEven[-2:]}'

    return date_str


