import setuptools

with open('VERSION.txt', 'r') as f:
    version = f.read().strip()

setuptools.setup(
    name="odoo14-addons-akretion-bank-statement-import-ebury",
    description="Meta package for akretion-bank-statement-import-ebury Odoo addons",
    version=version,
    install_requires=[
        'odoo14-addon-account_statement_import_ebury',
    ],
    classifiers=[
        'Programming Language :: Python',
        'Framework :: Odoo',
        'Framework :: Odoo :: 14.0',
    ]
)
