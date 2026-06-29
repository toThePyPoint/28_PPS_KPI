from datetime import date, timedelta


# Python: Monday=0 ... Sunday=6
TRANSPORT_ROUTES = {
    # Polska -> Francja
    ("2101", "1201"): [
        {"ship_day": 2, "arrival_day": 2},  # śr -> wt (+ 1 dzień na przetwarzanie)
        {"ship_day": 4, "arrival_day": 3},  # pt -> śr (+ 1 dzień na przetwarzanie)
    ],

    # Polska -> Czechy
    ("2101", "3701"): [
        {"ship_day": 1, "arrival_day": 3},  # wt -> czw
        {"ship_day": 4, "arrival_day": 1},  # pt -> wt
    ],

    # Polska -> Niemcy
    ("2101", "0301"): [
        {"ship_day": 0, "arrival_day": 3},  # pn -> czw
        {"ship_day": 1, "arrival_day": 4},  # wt -> pt
        {"ship_day": 2, "arrival_day": 0},  # śr -> pon
        {"ship_day": 3, "arrival_day": 1},  # czw -> wt
        {"ship_day": 4, "arrival_day": 2},  # pt -> śr

    ],

        # Niemcy -> Polska
    ("0301", "2101"): [
        {"ship_day": 0, "arrival_day": 2},  # pn -> śr
        {"ship_day": 1, "arrival_day": 3},  # wt -> czw
        {"ship_day": 2, "arrival_day": 4},  # śr -> pt
        {"ship_day": 3, "arrival_day": 0},  # czw -> pon
        {"ship_day": 4, "arrival_day": 1},  # pt -> wt

    ],

        # Niemcy -> Czechy
    ("0301", "3701"): [
        {"ship_day": 1, "arrival_day": 2},  # wt -> śr
        {"ship_day": 3, "arrival_day": 4},  # czw -> pt
    ],

        # Niemcy -> Francja
    ("0301", "1201"): [
        {"ship_day": 1, "arrival_day": 3},  # wt -> śr (+ 1 dzień na przetwarzanie)
        {"ship_day": 3, "arrival_day": 0},  # czw -> pt (+ 1 dzień na przetwarzanie)
    ],
}


def get_production_shipping_date(
        customer_ship_date: date,
        production_plant: str,
        destination_plant: str
) -> date:
    """
    Zwraca datę wysyłki z zakładu produkcyjnego.

    customer_ship_date - data wysyłki do klienta
    production_plant - kod zakładu produkcyjnego
    destination_plant - kod magazynu wysyłkowego
    """

    # Ten sam zakład -> brak transportu
    if production_plant == destination_plant:
        return customer_ship_date

    route = TRANSPORT_ROUTES.get((production_plant, destination_plant))

    if route is None:
        raise ValueError(
            f"Brak zdefiniowanej trasy {production_plant} -> {destination_plant}"
        )

    candidates = []

    for leg in route:
        arrival_day = leg["arrival_day"]
        ship_day = leg["ship_day"]

        # ile dni cofnąć do ostatniego wystąpienia dnia dostawy
        days_back = (customer_ship_date.weekday() - arrival_day) % 7

        arrival_date = customer_ship_date - timedelta(days=days_back)

        # obliczenie daty załadunku odpowiadającej tej dostawie
        transit_days = (arrival_day - ship_day) % 7
        if transit_days == 0:
            transit_days = 7

        production_ship_date = arrival_date - timedelta(days=transit_days)

        candidates.append(production_ship_date)

    # wybieramy najpóźniejszy możliwy załadunek
    return max(candidates)