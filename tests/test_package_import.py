import avrocli


def test_package_metadata():
    assert avrocli.__author__
    assert avrocli.__email__
    assert avrocli.__version__
