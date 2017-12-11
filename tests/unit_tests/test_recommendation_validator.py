from analytics_platform.kronos.src.recommendation_validator import RecommendationValidator

from unittest import TestCase


class TestRecommendationValidator(TestCase):

    def __init__(self, *args, **kwargs):
        super(TestRecommendationValidator, self).__init__(*args, **kwargs)
        self.input_folder_name = "tests/data/data_recom_valid"
        self.input_ecosystem = "maven"
        self.additional_path = ""

    def test_load_manifest_file(self):
        obj = RecommendationValidator.load_package_list_local(
            self.input_folder_name, self.additional_path, self.input_ecosystem)
        self.assertTrue(obj is not None)
        self.assertTrue(isinstance(obj, RecommendationValidator))
        self.assertTrue(obj.manifest_len == 4)

    def test_filter_input_list(self):
        obj = RecommendationValidator.load_package_list_local(
            self.input_folder_name, self.additional_path, self.input_ecosystem)
        input_list = ['A', 'B', 'C', 'Z']
        missing_packages = ['Z']
        filtered_input_list = obj.get_filtered_input_list(
            input_list, missing_packages)
        self.assertTrue(filtered_input_list == ['A', 'B', 'C'])

    def test_filter_alternate_list(self):
        obj = RecommendationValidator.load_package_list_local(
            self.input_folder_name, self.additional_path, self.input_ecosystem)
        outlier_packages = [{
            "outlier_prbability": 0.99434381708188857,
            "package_name": "A",
            "topic_list": [
                "some_topic",
                "some_topic"
            ]
        }]
        alternate_packages = {
            "B": [
                {
                    "package_name": "Q",
                    "similarity_score": 0.75,
                    "topic_list": ["some_topic"]
                },
                {
                    "package_name": "R",
                    "similarity_score": 0.75,
                    "topic_list": ["some_topic"]
                }
            ],
            "C": [
                {
                    "package_name": "Q",
                    "similarity_score": 0.75,
                    "topic_list": ["some_topic"]
                },
                {
                    "package_name": "R",
                    "similarity_score": 0.75,
                    "topic_list": ["some_topic"]
                }
            ],
            "A": [
                {
                    "package_name": "Q",
                    "similarity_score": 0.75,
                    "topic_list": ["some_topic"]
                },
                {
                    "package_name": "R",
                    "similarity_score": 0.75,
                    "topic_list": ["some_topic"]
                }
            ]
        }
        filtered_alternate_list = obj.get_filtered_alternate_list(
            alternate_packages, outlier_packages)
        self.assertTrue(len(filtered_alternate_list) == len(outlier_packages))
        self.assertTrue(outlier_packages[0][
                        'package_name'] == list(filtered_alternate_list.keys())[0])

    def test_generate_companion_set(self):
        obj = RecommendationValidator.load_package_list_local(
            self.input_folder_name, self.additional_path, self.input_ecosystem)
        input_list = ['A', 'B', 'C']
        companion_package = 'D'
        comp_set = obj.generate_companion_dependency_set(
            input_list, companion_package)
        self.assertEqual(comp_set, {'A', 'B', 'C', 'D'})

    def test_generate_alternate_set(self):
        obj = RecommendationValidator.load_package_list_local(
            self.input_folder_name, self.additional_path, self.input_ecosystem)
        input_list = ['A', 'B', 'C']
        alternate_package = 'B123'
        alternate_to = 'B'
        alt_set = obj.generate_alternate_dependency_set(
            input_list, alternate_package, alternate_to)
        self.assertEqual(alt_set, {'A', 'B123', 'C'})

    def test_alternate_recommendation(self):
        obj = RecommendationValidator.load_package_list_local(
            self.input_folder_name, self.additional_path, self.input_ecosystem)
        recommended_dependency_set = {'A', 'F', 'C', 'D'}
        count = obj.check_companion_or_alternate_recommendation_validity(
            recommended_dependency_set)
        self.assertEqual(count, 2)
        recommended_dependency_set = {'A', 'W', 'C', 'D'}
        count = obj.check_companion_or_alternate_recommendation_validity(
            recommended_dependency_set)
        self.assertEqual(count, 0)

    def test_companion_recommendation(self):
        obj = RecommendationValidator.load_package_list_local(
            self.input_folder_name, self.additional_path, self.input_ecosystem)
        recommended_dependency_set = {'A', 'B', 'C', 'D', 'E'}
        count = obj.check_companion_or_alternate_recommendation_validity(recommended_dependency_set)
        self.assertEqual(count, 2)
        recommended_dependency_set = {'A', 'B', 'C', 'D', 'Q'}
        count = obj.check_companion_or_alternate_recommendation_validity(recommended_dependency_set)
        self.assertEqual(count, 0)

    def test_check_alternate_recommendation(self):
        obj = RecommendationValidator.load_package_list_local(
            self.input_folder_name, self.additional_path, self.input_ecosystem)
        input_list = ['A', 'B', 'C', 'D']
        alternate_packages = {
            "B": [
                {
                    "package_name": "E",
                    "similarity_score": 0.75,
                    "topic_list": ["some_topic"]
                },
                {
                    "package_name": "P",
                    "similarity_score": 0.75,
                    "topic_list": ["some_topic"]
                }
            ]
        }
        result_alternate_list = obj.check_alternate_recommendation(
            input_list, alternate_packages)
        self.assertTrue(len(result_alternate_list) == 1)
        self.assertTrue(result_alternate_list.get(
            'B', [{'test': 'failed'}])[0].get('package_name') == 'E')
        self.assertTrue(result_alternate_list.get(
            'B', [{'test': 'failed'}])[0].get('similarity_score') == 2)

    def test_check_companion_recommendation(self):
        obj = RecommendationValidator.load_package_list_local(
            self.input_folder_name, self.additional_path, self.input_ecosystem)
        input_list = ['A', 'B', 'C', 'D']
        companion_packages = [
            {
                "cooccurrence_probability": 0.49942501008907109,
                "package_name": "E",
                "topic_list": ["some_topic"]
            },
            {
                "cooccurrence_probability": 0.42699967636612329,
                "package_name": "F",
                "topic_list": ["some_topic"]
            },
            {
                "cooccurrence_probability": 0.42699967636612329,
                "package_name": "G",
                "topic_list": ["some_topic"]
            }
        ]

        result_companion_list = obj.check_companion_recommendation(
            input_list, companion_packages)
        self.assertTrue(len(result_companion_list) == 2)
        comp_name = [c.get('package_name') for c in result_companion_list]
        self.assertEqual(set(comp_name), {'E', 'F'})
