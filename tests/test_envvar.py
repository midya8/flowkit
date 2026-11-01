import unittest

from flowkit.envvar import check_syntax, expand, missing


class TestCheckSyntax(unittest.TestCase):
    def test_plain_text_passes(self):
        check_syntax("make all")

    def test_valid_forms_pass(self):
        check_syntax("run $A ${B} ${C:-default} $$ and $")

    def test_unclosed_brace_rejected(self):
        with self.assertRaises(ValueError):
            check_syntax("run ${OPEN")

    def test_empty_name_rejected(self):
        with self.assertRaises(ValueError):
            check_syntax("run ${}")

    def test_bad_name_rejected(self):
        with self.assertRaises(ValueError):
            check_syntax("run ${9LIVES}")


class TestExpand(unittest.TestCase):
    def test_bare_and_braced_forms(self):
        env = {"A": "1", "B": "two words"}
        self.assertEqual(expand("run $A and ${B}", env), "run 1 and two words")

    def test_unset_expands_to_empty(self):
        self.assertEqual(expand("[$NOPE]", {}), "[]")

    def test_default_used_when_unset(self):
        self.assertEqual(expand("${MISSING:-fallback}", {}), "fallback")

    def test_default_ignored_when_set(self):
        self.assertEqual(expand("${A:-fallback}", {"A": "real"}), "real")

    def test_empty_string_counts_as_set(self):
        self.assertEqual(expand("${A:-fallback}", {"A": ""}), "")

    def test_double_dollar_left_for_shell(self):
        self.assertEqual(expand("echo $$", {}), "echo $$")

    def test_dollar_before_non_name_is_literal(self):
        self.assertEqual(expand("pay $5 now", {}), "pay $5 now")

    def test_name_boundaries(self):
        self.assertEqual(expand("$A-suffix", {"A": "v"}), "v-suffix")


class TestMissing(unittest.TestCase):
    def test_reports_unset_names_without_defaults(self):
        env = {"SET": "1"}
        self.assertEqual(missing("$SET ${UNSET} ${FALLBACK:-x} $ALSO_UNSET", env),
                         ["UNSET", "ALSO_UNSET"])

    def test_no_duplicates(self):
        self.assertEqual(missing("$A $A ${A}", {}), ["A"])

    def test_all_set(self):
        self.assertEqual(missing("$A", {"A": "1"}), [])


if __name__ == "__main__":
    unittest.main()
