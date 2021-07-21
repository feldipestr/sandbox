import setuptools

setuptools.setup(
    name='dota-2-gsi-events-streamer',
    version='1.0',
    install_requires=['google-cloud-datastore==2.1.4', 'python-logstash==0.4.6'],
    packages=setuptools.find_packages(),
)