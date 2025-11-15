from datetime import datetime, timedelta


def next_run_from_cron(cron_expr: str, now: datetime) -> datetime:
    """
    CRON simplifié :
    - "* * * * *"    → toutes les minutes
    - "*/N * * * *"  → toutes les N minutes
    - "M * * * *"    → minute M chaque heure (ex: "0 * * * *")
    """
    parts = cron_expr.split()
    if len(parts) != 5:
        raise ValueError(f"Expression CRON invalide : {cron_expr}")

    minute, hour, day, month, weekday = parts

    # 1) */N
    if minute.startswith("*/"):
        interval = int(minute.replace("*/", ""))
        next_time = (now + timedelta(minutes=1)).replace(second=0, microsecond=0)
        while next_time.minute % interval != 0:
            next_time += timedelta(minutes=1)
        return next_time

    # 2) minute "*" → toutes les minutes
    if minute == "*":
        return (now + timedelta(minutes=1)).replace(second=0, microsecond=0)

    # 3) minute fixe : "0", "15", "32", etc.
    if minute.isdigit():
        m = int(minute)
        next_time = now.replace(second=0, microsecond=0)

        if now.minute < m:
            # encore dans l'heure courante
            return next_time.replace(minute=m)
        else:
            # prochain cycle → heure suivante
            return (next_time + timedelta(hours=1)).replace(minute=m)

    # Si on arrive ici → non supporté
    raise NotImplementedError(f"Format CRON non encore supporté : {cron_expr}")
