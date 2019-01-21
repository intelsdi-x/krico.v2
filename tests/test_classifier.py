import mock
import krico.analysis.classifier


@mock.patch.dict(krico.analysis.classifier._configuration, {'minimal_samples': 40})
class TestEnoughSamples(object):

    @mock.patch('krico.database.ClassifierInstance')
    def test_if_not_enough_samples(self, mock_classifier_instance):
        mock_classifier_instance.objects.filter.return_value.allow_filtering.return_value.count.return_value = 0
        assert krico.analysis.classifier._enough_samples('category', 'configuration_id') is False

    @mock.patch('krico.database.ClassifierInstance')
    def test_if_enough_samples(self, mock_classifier_instance):
        mock_classifier_instance.objects.filter.return_value.allow_filtering.return_value.count.return_value = 80
        assert krico.analysis.classifier._enough_samples('category', 'configuration_id') is True


class TestClassifier(object):

    def test_if_model_is_created_after_object_initialization(self):
        assert krico.analysis.classifier._Classifier('configuration_id').model

    def test_if_model_is_compiled_after_object_initialization(self):
        assert krico.analysis.classifier._Classifier('configuration_id').model.built is True

    def test_if_classifier_have_configuration_id(self):
        assert krico.analysis.classifier._Classifier('configuration_id').configuration_id == "configuration_id"

    def test_if_classifier_have_x_maxima(self):
        assert krico.analysis.classifier._Classifier('configuration_id').x_maxima == []
