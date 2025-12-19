from setuptools import setup, find_packages

setup(
    name='gdacs-pipeline',
    version='1.0.0',
    description='Real-time streaming + batch pipeline for GDACS events',
    author='Data Team',
    packages=find_packages(),
    python_requires='>=3.8',
    install_requires=[
        'apache-airflow==2.7.3',
        'apache-airflow-providers-apache-kafka==1.2.1',
        'kafka-python==2.0.2',
        'requests==2.31.0',
        'pandas==2.1.3',
        'python-dotenv==1.0.0',
        'SQLAlchemy==2.0.23',
    ],
    entry_points={
        'console_scripts': [
            'gdacs-producer=src.job1_producer:run_producer',
            'gdacs-cleaner=src.job2_cleaner:process_batch',
            'gdacs-analytics=src.job3_analytics:run_daily_analytics',
        ],
    },
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
    ],
)
