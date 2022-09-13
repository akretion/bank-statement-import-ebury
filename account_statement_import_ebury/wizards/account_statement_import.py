# Copyright 2022 Akretion France (http://www.akretion.com/)
# @author: Alexis de Lattre <alexis.delattre@akretion.com>
# License AGPL-3.0 or later (https://www.gnu.org/licenses/agpl).

from odoo import api, models
import io
import logging
import csv
import zipfile
from tempfile import TemporaryDirectory
import glob
import os
from collections import defaultdict

_logger = logging.getLogger(__name__)


class AccountStatementImport(models.TransientModel):
    _inherit = "account.statement.import"

    @api.model
    def _check_ebury(self, data_file):
        tmpdir = TemporaryDirectory(prefix='odoo-ebury')
        try:
            eburyzip = zipfile.ZipFile(io.BytesIO(data_file))
            eburyzip.extractall(tmpdir.name)
        except Exception as e:
            _logger.debug('Check ebury: failed to unzip: %s', e)
            return False
        list_files = glob.glob(tmpdir.name + "/*.csv")
        if len(list_files) >= 3:
            prefixset = set()
            res = {}
            for csvfile in list_files:
                filename = os.path.basename(csvfile)
                filename_split = filename.split('-')
                if len(filename_split) != 4:
                    _logger.info(
                        "The filename %s contains %d '-' instead of 4.",
                        filename, len(filename_split))
                    return False
                prefix = '-'.join(filename_split[:3])
                acc_number = filename_split[2]
                if res.get('acc_number') and acc_number != res['acc_number']:
                    _logger.info(
                        'The ZIP archive contains different account numbers '
                        '(%s and %s).', res.get('acc_number'), acc_number)
                    return False
                res['acc_number'] = acc_number
                prefixset.add(prefix)
                if filename.endswith('-all_currencies.csv'):
                    res['file'] = csvfile
            if len(prefixset) == 1 and res.get('acc_number') and res.get('file'):
                res['data'] = []
                with open(res['file']) as f:
                    i = 0
                    for line in csv.DictReader(f):
                        i += 1
                        _logger.debug('line %d=%s', i, line)
                        res['data'].append(line)
                return res
            else:
                return False
        else:
            _logger.info(
                'The ZIP archive contains %d csv files, instead of >= 3',
                len(list_files))
            return False

    @api.model
    def _prepare_ebury_transaction_line(self, line):
        unique_import_id = '-'.join(
            [line['Timestamp'].replace(' ', '_'), line['Amount']])
        vals = {
            "date": line['Timestamp'][:10],
            "payment_ref": line['Description'],
            "amount": float(line['Amount']),
            "unique_import_id": unique_import_id,
        }
        return vals

    def _parse_file(self, data_file):
        res = self._check_ebury(data_file)
        if not res:
            return super()._parse_file(data_file)
        result = []
        # from pprint import pprint
        # pprint(res['data'])
        cur2trans = defaultdict(list)
        for line in res['data']:
            cur2trans[line['Currency']].append(line)
        for currency_code, lines in cur2trans.items():
            lines_sorted = sorted(lines, key=lambda x: x['Timestamp'])
            transactions = []
            balance_start = None
            balance_end_real = None
            journal_acc_number = '-'.join([res['acc_number'], currency_code])
            for line in lines_sorted:
                vals = self._prepare_ebury_transaction_line(line)
                if vals:
                    transactions.append(vals)
                    line_bal = float(line['Balance'])
                    if balance_start is None:
                        balance_start = line_bal - vals["amount"]
                    balance_end_real = line_bal
            vals_bank_statement = {
                "name": res['acc_number'],
                "transactions": transactions,
                "balance_start": balance_start or 0.0,
                "balance_end_real": balance_end_real or 0.0,
            }
            result.append((currency_code, journal_acc_number, [vals_bank_statement]))
        # pprint(result)
        return result
