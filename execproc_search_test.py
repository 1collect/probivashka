#!/usr/bin/env python3
import unittest

from execproc_search import extract_legal_basis


class ExtractLegalBasisTests(unittest.TestCase):
    def test_extracts_kazakh_subpoint_written_with_closing_parenthesis(self) -> None:
        text = (
            "негізінде қозғалған (борышкер КАЛАУОВ АСХАТ ЕЛАМАНОВИЧ) "
            "атқарушылық іс жүргізу материалдарын қарап, АНЫҚТАДЫ: күші жойылды "
            "Жоғарыда аталғанның негізінде, Қазақстан Республикасының 2010 жылғы "
            "2 сәуірдегі Заңының 10- бабының 1-тармағын, "
            "47-бабы 1-тармағының 5) тармақшасын, 126-бабын "
            "басшылыққа ала отырып"
        )

        self.assertEqual(
            extract_legal_basis(text),
            "47-бабы 1-тармағының 5-тармақшасын",
        )

    def test_kazakh_fallback_starts_at_closing_legal_formula(self) -> None:
        text = (
            "негізінде қозғалған іс туралы ұзақ кіріспе. "
            "Жоғарыда аталғанның негізінде, Заңды басшылыққа ала отырып"
        )

        self.assertEqual(
            extract_legal_basis(text),
            "Жоғарыда аталғанның негізінде, Заңды басшылыққа ала отырып",
        )


if __name__ == "__main__":
    unittest.main()
