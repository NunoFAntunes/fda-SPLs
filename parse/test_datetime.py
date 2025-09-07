from normalize.normalizers.date_normalizer import DateNormalizer

normalizer = DateNormalizer()

test_dates = ['2024-03-15T10:30:00', '20240315103000']
for date_str in test_dates:
    normalized = normalizer.normalize_date(date_str)