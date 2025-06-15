from dataclasses import dataclass


@dataclass(slots=True)
class CountryMapProjection:
    center: tuple[float, float]
    scale: float
