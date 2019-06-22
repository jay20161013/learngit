import base64
import re
import time
import datetime
import urllib
from dateutil.relativedelta import relativedelta
from django.db.models import Q, Sum
import requests
from django_pandas.io import read_frame
import numpy as np
import pandas as pd
from odo import odo
import sqlalchemy
import tablib
import xmltodict
from bj.models import Sku, Sale
from dw.etl import EtlBase, OpEtl, hdDataError
from system.util import ENGINE, ParameterError
from system.models import Dict
from yy.admin import IcbcSlipResource, IcbcGcardResource, IcbcGslipResource
from yy.analy import AnaAcctBook, AnaAge, AnaShortBorrow
from yy.models import *
from django.core.exceptions import ObjectDoesNotExist

from sqlalchemy.exc import OperationalError
from django.db import IntegrityError
from sqlalchemy.exc import DatabaseError
from yy.ops import OpGLAccsumAdd

lDefine = ['EtlCode', 'EtlDept', 'EtlVendor']
lData = ['EtlGLAccsum', 'EtlGLAccass', 'EtlGLAccvouch', 'EtlIASummary', 'EtlInventory', 'EtlAcctBook',
         'EtlAcctBookFull', 'EtlShortBorrowFull', 'EtlPayAge']
lRitual = ['EtlIcbcSlip', 'EtlIcbcGslip']


class EtlYy(EtlBase):
    def __init__(self, iAccid, iYear=None, iMonth=None):
        self.iAccid = iAccid
        super(EtlYy, self).__init__(iYear, iMonth)
        self.oDAccid = Dict.objects.get(syscode='accid', value=iAccid)
        self.oDYear = Dict.objects.get(syscode='year', value=self.iYear)
        self.sParameter = str(self.iAccid) + '.' + str(self.iYear) + '.' + str(self.iMonth)

    @property
    def iAccid(self):
        return self._iAccid

    @iAccid.setter
    def iAccid(self, a):
        try:
            Dict.objects.get(syscode='accid', value=a)
        except ObjectDoesNotExist:
            raise ParameterError("No such accid.")
        self._iAccid = a

    @EtlBase.iYear.setter
    def iYear(self, y):
        super(EtlYy, self.__class__).iYear.fset(self, y)
        try:
            ENGINE.UFIDA(self.iAccid, y).execute('SELECT 1')
        except OperationalError:
            raise ParameterError("No such year about accid:{}".format(self.iAccid))
        self._iYear = y

    def getCodeId(self, x):
        if x is None:
            return None
        lCode = re.findall('..', x)
        lId = [self.oDAccid.value, self.oDYear.value] + [lCode[0] + lCode[1]] + lCode[2:]
        return '_'.join(lId)

    def getDeptId(self, x):
        if x is None:
            return None
        lDept = re.findall('..', x)
        lDept = lDept if len(lDept) else [x]
        lId = [self.oDAccid.value, self.oDYear.value] + lDept
        return '_'.join(lId)

    def getAccId(self, x):
        oAccid = Dict.objects.get(syscode='accid', value=x)
        return oAccid.id


class EtlCode(EtlYy):
    def __init__(self, iAccid, iYear=None, iMonth=None):
        super(EtlCode, self).__init__(iAccid, iYear, iMonth)
        self.sParameter = str(self.iAccid) + '.' + str(self.iYear)

    def sync(self):
        dfCode = pd.read_sql("""SELECT i_id, cclass, ccode, LEFT(ccode, 4) + ccode_name ccode_name, igrade, bproperty, bend
                                FROM code ORDER BY ccode""", ENGINE.UFIDA(self.iAccid, self.iYear))

        # get accid root node
        if Code.objects.filter(id=self.iAccid):
            oCAccid = Code.objects.get(id=self.iAccid)
        else:
            oCAccid = Code.objects.create(id=self.iAccid, i_id=0, cclass='', ccode='', ccode_name='', bend=0,
                                          igrade=0, bproperty=1, accid=self.oDAccid, year=None, parent=None)

        # Code.objects.filter(accid=self.oDAccid, year=self.oDYear).delete()
        ENGINE.SCMSDW.execute("DELETE YY_CODE WHERE accid_id='{}' AND year_id='{}'".format(self.oDAccid.id, self.oDYear.id))
        tmpCode = Code.objects.create(id=oCAccid.id + '_' + self.oDYear.value, i_id=0, cclass='', ccode='', ccode_name='', bend=0,
                                      igrade=0, bproperty=1, accid=self.oDAccid, year=self.oDYear, parent=oCAccid)

        # insert codeadd
        for ca in CodeAdd.objects.filter(parent=tmpCode.id):
            Code.objects.create(id=ca.id, i_id=0, cclass=ca.cclass, ccode=ca.ccode, ccode_name=ca.ccode_name, bend=ca.bend,
                                igrade=ca.igrade, bproperty=ca.bproperty, accid=self.oDAccid, year=self.oDYear, parent=tmpCode)

        dfCode['id'] = dfCode['ccode'].apply(self.getCodeId)

        for i, c in dfCode.iterrows():
            parent = Code.objects.get(id=c.id[:re.search('_[\d-]+$', c.id).span()[0]])
            tmpCode = Code.objects.create(id=c.id, i_id=c.i_id, cclass=c.cclass, ccode=c.ccode, ccode_name=c.ccode_name, bend=c.bend,
                                  igrade=c.igrade, bproperty=c.bproperty, accid=self.oDAccid, year=self.oDYear, parent=parent)

            # insert codeadd
            for ca in CodeAdd.objects.filter(parent=tmpCode.id):
                Code.objects.create(id=ca.id, i_id=0, cclass=ca.cclass, ccode=ca.ccode, ccode_name=ca.ccode_name, bend=ca.bend,
                                    igrade=ca.igrade, bproperty=ca.bproperty, accid=self.oDAccid, year=self.oDYear, parent=tmpCode)

        # set code map
        # UPDATE YY_CODE SET adjust=SUBSTR(ccode,1,4)||ccode_name
        # WHERE adjust IS NULL AND id LIKE '{ay}%';
        sqlUpdate = """BEGIN
                       FOR CR IN (SELECT a.ROWID, b.adjust, a.ccode_name
                       FROM yy_code a, yy_codeadjust b
                       WHERE a.id LIKE b.code_id||'%' AND b.iedition = {y}
                       AND b.adjust IS NOT NULL AND a.id LIKE '{ay}%') LOOP
                       UPDATE YY_CODE SET
                       adjust = CR.adjust
                       WHERE ROWID = CR.ROWID;
                       END LOOP;
                       INSERT INTO YY_CODEADJUST(Code_Id, Iedition, i_Id, Cclass, Ccode, Ccode_Name, Igrade, Adjust)
                       SELECT a.id code_id, {y} iedition, a.i_id, a.cclass, a.ccode, a.ccode_name, a.igrade, 'wait_arange' adjust
                       FROM YY_CODE a, SYSTEM_DICT b WHERE a.accid_id = b.id AND b.syscode = 'accid'
                       AND a.year_id = {y} AND a.igrade = 2 AND b.parent_id IN ('accid_sh', 'accid_wz')
                       AND NOT EXISTS (SELECT 1 FROM YY_CODEADJUST c WHERE c.code_id = a.id);
                     END;""".format(ay=str(self.iAccid) + '_' + str(self.iYear), y=self.iYear)
        ENGINE.SCMSDW.execute(sqlalchemy.text(sqlUpdate, autocommit=True))


class EtlDept(EtlYy):
    def __init__(self, iAccid, iYear=None, iMonth=None):
        super(EtlDept, self).__init__(iAccid, iYear, iMonth)
        self.sParameter = str(self.iAccid) + '.' + str(self.iYear)

    def sync(self):
        dfDepartment = pd.read_sql("SELECT cDepCode, bDepEnd, cDepName, iDepGrade FROM Department ORDER BY cDepCode",
                             ENGINE.UFIDA(self.iAccid, self.iYear))
        # get accid root node
        if Department.objects.filter(id=self.iAccid):
            oDepartmentAccid = Department.objects.get(id=self.iAccid)
        else:
            oDepartmentAccid = Department.objects.create(id=self.iAccid, cdepcode='', bdepend=0, cdepname='',
                                             idepgrade=0, accid=self.oDAccid, year=None, parent=None)

        # Department.objects.filter(accid=self.oDAccid, year=self.oDYear).delete()
        ENGINE.SCMSDW.execute("DELETE YY_Department WHERE accid_id='{}' AND year_id='{}'".format(self.oDAccid.id, self.oDYear.id))
        oDepartmentYear = Department.objects.create(id=oDepartmentAccid.id + '_' + self.oDYear.value, cdepcode='', bdepend=0, cdepname='',
                                        idepgrade=0, accid=self.oDAccid, year=self.oDYear, parent=oDepartmentAccid)

        dfDepartment['id'] = dfDepartment['cDepCode'].apply(self.getDeptId)

        for i, c in dfDepartment.iterrows():
            parent = Department.objects.get(id=c.id[:re.search('_[a-z0-9A-Z]+$', c.id).span()[0]])
            Department.objects.create(id=c.id, cdepcode=c.cDepCode, bdepend=c.bDepEnd, cdepname=c.cDepName,
                                      idepgrade=c.iDepGrade, accid=self.oDAccid, year_id=self.iYear, parent=parent)

        # set code map
        # UPDATE YY_DEPARTMENT SET adjust=cdepname
        # WHERE adjust IS NULL AND id LIKE '{ay}%';
        sqlUpdate = """BEGIN
                       FOR CR IN (SELECT a.ROWID, b.adjust, a.cdepname
                       FROM yy_department a, yy_departmentadjust b
                       WHERE a.id LIKE b.department_id||'%' AND b.iedition = {y}
                       AND b.adjust IS NOT NULL AND a.id LIKE '{ay}%') LOOP
                       UPDATE YY_DEPARTMENT SET
                       adjust = CR.adjust
                       WHERE ROWID = CR.ROWID;
                       END LOOP;

                       UPDATE  yy_department a SET a.cdepname = regexp_replace(a.cdepname, '）', ')') WHERE a.cdepname LIKE '%）%';
                       UPDATE  yy_department a SET a.cdepname = regexp_replace(a.cdepname, '（', '(') WHERE a.cdepname LIKE '%（%';

                       INSERT INTO YY_DEPARTMENTADJUST(Iedition, cdepcode, cdepname, bdepend, idepgrade, adjust, DEPARTMENT_ID)
                       SELECT a.year_id IEDITION, a.cdepcode, a.cdepname, a.bdepend, a.idepgrade, 'wait_arange' adjust, a.id DEPARTMENT_ID
                       FROM YY_DEPARTMENT a, SYSTEM_DICT b
                       WHERE a.cdepcode IS NOT NULL AND a.accid_id = b.id AND b.syscode = 'accid'
                       AND a.year_id = {y} AND b.parent_id IN ('accid_sh', 'accid_wz')
                       AND NOT EXISTS (SELECT 1 FROM YY_DEPARTMENTADJUST c WHERE c.DEPARTMENT_ID = a.id);
                     END;""".format(ay=str(self.iAccid) + '_' + str(self.iYear), y=self.iYear)
        ENGINE.SCMSDW.execute(sqlalchemy.text(sqlUpdate, autocommit=True))


class EtlVendor(EtlYy):
    def __init__(self, iAccid, iYear=None, iMonth=None):
        super(EtlVendor, self).__init__(iAccid, iYear, iMonth)
        self.sParameter = str(self.iAccid) + '.' + str(self.iYear)

    def sync(self):
        sAccid = Dict.objects.get(syscode='accid', value=self.iAccid).id
        sqlVendor = """SELECT '{accid}_{year}_'+a.cVenCode id, '{sAccid}' accid_id, '{year}' year_id,
        a.cVenName cvenname, a.cVenAbbName cvenabbname FROM Vendor a""".format(accid=self.iAccid, year=self.iYear, sAccid=sAccid)
        dfVendor = pd.read_sql(sqlVendor, ENGINE.UFIDA(self.iAccid, self.iYear))

        ENGINE.SCMSDW.execute("DELETE YY_VENDOR WHERE accid_id = '{}' AND year_id = {}".format(sAccid, self.iYear))
        odo(dfVendor, ENGINE.CONSTR_SCMSDW + '::YY_VENDOR')


class EtlGLAccsum(EtlYy):
    def sync(self):
        GLAccsum.objects.filter(code__accid__syscode='accid', code__accid__value=str(self.iAccid),
                                code__year_id=self.iYear, month_id=self.iMonth).delete()

        dfGLAccsum = pd.read_sql("""SELECT NULL id, a.i_id, a.ccode code_id, a.iperiod month_id,
        CASE WHEN a.cbegind_c = b.property THEN a.mb ELSE -a.mb END mb,
        CASE WHEN a.ccode = '3131' THEN a.md - a.mc ELSE a.md END md, a.mc,
        CASE WHEN a.cendd_c = c.property THEN a.me ELSE -a.me END me, a.cbegind_c, a.cendd_c, 'accountorg_0101' accountorg_id
        FROM GL_Accsum a,
        (SELECT b.ccode, b.bend, CASE b.bproperty WHEN 1 THEN '借' WHEN 0 THEN '贷' ELSE '借' END property FROM code b) b,
        (SELECT c.ccode, c.bend, CASE c.bproperty WHEN 1 THEN '借' WHEN 0 THEN '贷' ELSE '借' END property FROM code c) c
        WHERE a.ccode = b.ccode AND a.ccode = c.ccode AND b.bend = 1
        AND a.iperiod = {}""".format(self.iMonth), ENGINE.UFIDA(self.iAccid, self.iYear))

        dfGLAccsum['code_id'] = dfGLAccsum['code_id'].apply(self.getCodeId)
        dfGLAccsum['date_id'] = self.dEnd
        odo(dfGLAccsum, ENGINE.CONSTR_SCMSDW + '::YY_GLACCSUM')


        lAccidRange = ['accid_sh', 'accid_wz']
        if self.oDAccid.get_ancestors().last().id in lAccidRange:
            dCodeMap = {"5101网购": "5191网购", "5401网购": "5491网购"}
            cAccountOrg='accountorg_0302'
            OpGLAccsumAdd.addAccsumByCodeRange(self, lAccidRange, dCodeMap, cAccountOrg, [-1, 1])

            dCodeMap = {"5501客户广宣费补贴": "5101客户广宣费补贴", "5501客户现金补贴费用": "5101客户现金补贴费用"}
            cAccountOrg='accountorg_0401'
            OpGLAccsumAdd.addAccsumByCodeRange(self, lAccidRange, dCodeMap, cAccountOrg)

            dCodeMap = {"5101温州调至上海": "5401温州调至上海", "5101温州直接销售": "5401温州直接销售"}
            cAccountOrg='accountorg_0203'
            OpGLAccsumAdd.addAccsumByCodeRange(self, lAccidRange, dCodeMap, cAccountOrg)


        lAccidRange = ['accid_sh']
        cAccountOrg='accountorg_0303'
        if self.oDAccid.get_ancestors().last().id in lAccidRange:
            # ====================================================================================================
            md01 = GLAccass.objects.filter(code__accid__id__startswith='accid_sh', date__calendaryear=self.iYear, date__accountingmonth=self.iMonth,
            dept__cdepname__contains='营', code__ccode__in=['510204']).values('md').aggregate(md=Sum('md'))['md'] or 0
            md02 = GLAccass.objects.filter(code__accid__id__startswith='accid_sh', date__calendaryear=self.iYear, date__accountingmonth=self.iMonth,
            dept__cdepname__contains='营', code__ccode__in=['540502']).values('md').aggregate(md=Sum('md'))['md'] or 0

            dCodeMap = {"5102推广物料": "5405推广物料"}
            OpGLAccsumAdd.addAccsumByData(self, lAccidRange, dCodeMap, cAccountOrg, [md01, md02], [-1, -1], ['广宣收入', '广宣支出'])

            dCodeMap = {"5102推广物料": "5501公司广告宣传费"}
            OpGLAccsumAdd.addAccsumByData(self, lAccidRange, dCodeMap, cAccountOrg, [0, md01-md02], [0, -1])

            # ====================================================================================================
            dCodeMap = {"5102直营店利润":"5102废品收入"}
            OpGLAccsumAdd.addAccsumByCodeRange(self, lAccidRange, dCodeMap, cAccountOrg, [-1, 0])


        lAccidRange = ['accid_zy']
        cAccountOrg='accountorg_0303'
        if self.oDAccid.get_ancestors().last().id in lAccidRange:
            # ====================================================================================================
            dCodeMap = {"5301营业外收入": "5101销售收入"}
            OpGLAccsumAdd.addAccsumByCodeRange(self, lAccidRange, dCodeMap, cAccountOrg, [-1, 1])

            # ====================================================================================================
            md = Sale.objects.filter(buyer__typename='公司自营', sku__scodetype='主营业务',
            date__calendaryear=self.iYear, date__accountingmonth=self.iMonth).values('origamount')\
            .aggregate(origamount=Sum('origamount'))['origamount']

            dCodeMap = {"5101销售收入": "5401主营业务成本"}
            OpGLAccsumAdd.addAccsumByData(self, lAccidRange, dCodeMap, cAccountOrg, [md])

            # ====================================================================================================
            lo = IASummary.objects.filter(accid=self.oDAccid, year__id=self.dPreEnd.year, month__id=self.dPreEnd.month)
            lSum = lo.aggregate(Sum('imoney'), Sum('icost'))
            md01 = (lSum['imoney__sum'] or 0) - (lSum['icost__sum'] or 0)

            lo = IASummary.objects.filter(accid=self.oDAccid, year=self.oDYear, month__id=self.iMonth)
            lSum = lo.aggregate(Sum('imoney'), Sum('icost'))
            md02 = (lSum['imoney__sum'] or 0) - (lSum['icost__sum'] or 0)

            dCodeMap = {"5401主营业务成本": "5401主营业务成本"}
            OpGLAccsumAdd.addAccsumByData(self, lAccidRange, dCodeMap, cAccountOrg, [md01, md02], [-1, 1], ['减期初', '加期末'])


        lAccidRange = ['accid_sh']
        cAccountOrg='accountorg_0402'
        if self.oDAccid.get_ancestors().last().id in lAccidRange:
            dCodeMap = {"5101网购": "5401网购"}
            OpGLAccsumAdd.addAccsumByCodeRange(self, lAccidRange, dCodeMap, cAccountOrg, [-1, -1])


class EtlGLAccass(EtlYy):
    def sync(self):
        GLAccass.objects.filter(code__accid__syscode='accid', code__accid__value=self.iAccid,
                                code__year_id=self.iYear, month_id=self.iMonth).delete()

        dfGLAccass = pd.read_sql("""SELECT NULL id, a.i_id, a.ccode code_id, a.cdept_id dept_id, a.cperson_id,
        a.ccus_id, a.csup_id, a.citem_class, a.citem_id, a.iperiod month_id,
        CASE WHEN a.cbegind_c = b.property  THEN mb ELSE -mb END mb, a.md, a.mc,
        CASE WHEN a.cendd_c = c.property THEN me ELSE -me END me, a.cbegind_c, a.cendd_c
        FROM GL_Accass a,
        (SELECT b.ccode, CASE b.bproperty WHEN 1 THEN '借' WHEN 0 THEN '贷' ELSE '借' END property FROM code b) b,
        (SELECT c.ccode, CASE c.bproperty WHEN 1 THEN '借' WHEN 0 THEN '贷' ELSE '借' END property FROM code c) c
        WHERE a.ccode = b.ccode AND a.ccode = c.ccode
        AND a.iperiod = {}""".format(self.iMonth), ENGINE.UFIDA(self.iAccid, self.iYear))

        dfGLAccass['code_id'] = dfGLAccass['code_id'].apply(self.getCodeId)
        dfGLAccass.loc[~dfGLAccass['dept_id'].isnull(), 'dept_id'] = \
            dfGLAccass[~dfGLAccass['dept_id'].isnull()]['dept_id'].apply(self.getDeptId)
        dfGLAccass['date_id'] = self.dEnd
        odo(dfGLAccass, ENGINE.CONSTR_SCMSDW + '::yy_glaccass')


class EtlGLAccvouch(EtlYy):
    def sync(self):
        GLAccvouch.objects.filter(code__accid__syscode='accid', code__accid__value=self.iAccid,
                                  date__calendaryear=self.iYear, date__accountingmonth=self.iMonth).delete()

        dfGLAccvouch = pd.read_sql("""SELECT NULL id, a.i_id, a.iperiod month_id, a.csign, a.ino_id, a.inid, a.dbill_date date_id,
        a.csign + '-' + CAST(a.ino_id AS VARCHAR(5)) + ':' + a.cdigest cdigest, a.ccode code_id, a.md, a.mc, a.cdept_id department_id
        FROM GL_accvouch a WHERE iperiod = {}""".format(self.iMonth),
        ENGINE.UFIDA(self.iAccid, self.iYear))

        dfGLAccvouch['code_id'] = dfGLAccvouch['code_id'].apply(self.getCodeId)
        dfGLAccvouch['department_id'] = dfGLAccvouch['department_id'].apply(self.getDeptId)
        odo(dfGLAccvouch, ENGINE.CONSTR_SCMSDW + '::YY_GLACCVOUCH')


class EtlIASummary(EtlYy):
    def sync(self):
        chIASummary = pd.read_sql("SELECT '{}' accid_id,{} year_id,AutoID autoid, cWhCode cwhcode, "
                                  "cInvCode cinvcode,cDepCode dept_id,iMonth month_id,iINum iinum, "
                                  "iONum ionum,iNum inum,iIMoney iimoney,iOMoney iomoney,iMoney imoney, "
                                  "iUnitPrice iunitprice, 0 icost FROM IA_Summary a WHERE a.iMonth = {}"
                                  .format(self.iAccid, self.iYear, self.iMonth),
                                  ENGINE.UFIDA(self.iAccid, self.iYear), chunksize=1000)
        IASummary.objects.filter(accid_id=self.oDAccid.id, year_id=self.iYear, month_id=self.iMonth).delete()
        for dfIASummary in chIASummary:
            dfIASummary.loc[~dfIASummary['dept_id'].isnull(), 'dept_id'] = \
                dfIASummary[~dfIASummary['dept_id'].isnull()]['dept_id'].apply(self.getDeptId)
            dfIASummary['accid_id'] = self.oDAccid.id
            dfIASummary.replace([np.inf, -np.inf], np.nan, inplace=True)
            dfIASummary.fillna(0, inplace=True)
            dfIASummary['id'] = None
            # dfIASummary.drop(['iinum','ionum','inum','iunitprice'], inplace=True,axis=1)
            odo(dfIASummary, ENGINE.CONSTR_SCMSDW + '::YY_IASUMMARY')

        sqlUpdate="""BEGIN
                       FOR CR IN (SELECT a.ROWID, a.inum*b.cost cost
                       FROM YY_IASUMMARY a, YY_INVENTORY b
                       WHERE a.cinvcode = b.sku
                       AND a.year_id = {} AND a.month_id = {}
                       AND b.date_id = TO_DATE('{}', 'YYYY-MM-DD')) LOOP
                       UPDATE YY_IASUMMARY SET
                       icost = CR.cost
                       WHERE ROWID = CR.ROWID;
                       END LOOP;
                     END;""".format(self.iYear, self.iMonth, self.sEnd)
        ENGINE.SCMSDW.execute(sqlalchemy.text(sqlUpdate, autocommit=True))


class EtlInventory(EtlYy):
    @classmethod
    def getUnitPrice(cls, g):
        ionum = g['ionum'].sum()
        inum = g['inum'].sum()
        if ionum > 0:
            iunitprice = sum(g['iomoney']) / ionum
        elif inum > 0:
            iunitprice = g['imoney'].sum() / g['inum'].sum()
        else:
            iunitprice = 0
        return float(iunitprice)

    def sync(self):
        """
            不含制造费用及人工
        """
        if not self.oDAccid.id.count('sh'):
            return
        dPreDate = datetime.date(self.iYear, self.iMonth, 1) + relativedelta(months=-1)
        iPreYear = dPreDate.year
        iPreMonth = dPreDate.month

        loDAccid = Dict.objects.filter(syscode='accid', parent__name__in=['上海', '温州'])

        dfIASummary = read_frame(IASummary.objects.filter(accid__in=loDAccid, year=self.iYear, month=self.iMonth)\
                                 .filter(~Q(imoney=0) | ~Q(iunitprice=0)),
                                 fieldnames=['accid__id', 'cinvcode', 'ionum', 'iomoney', 'inum', 'imoney'])

        dfInvCost = pd.DataFrame(dfIASummary.groupby('cinvcode').apply(self.getUnitPrice), columns=['cost'])
        dfInvCost = dfInvCost[dfInvCost['cost'] > 0]
        dfInvCost['realdate'] = self.dEnd
        dfInvCost.index.name = 'sku'

        dfPreInvCost = read_frame(Inventory.objects.filter(date__calendaryear=iPreYear,
                                                           date__accountingmonth=iPreMonth),
                                  index_col='sku', fieldnames=['date__id', 'cost'])
        dfMerge = pd.merge(dfInvCost, dfPreInvCost, left_index=True, right_index=True, suffixes=['', '_old'], how='outer')
        indexs = dfMerge['cost'].isnull()
        dfMerge.loc[indexs, 'cost'] = dfMerge.loc[indexs, 'cost_old']
        dfMerge.loc[indexs, 'realdate'] = dfMerge.loc[indexs, 'date__id']
        dfMerge = dfMerge[~dfMerge['realdate'].isnull()]
        dfMerge.drop(['date__id', 'cost_old'], axis=1, inplace=True)

        dfSku = read_frame(Sku.objects.all(), index_col='id', fieldnames=['fprice'])

        dfInventory = pd.merge(dfMerge, dfSku, left_index=True, right_index=True, how='left')
        dfInventory.reset_index(inplace=True)
        dfInventory['id'] = None
        dfInventory['name'] = ''
        dfInventory['fprice'] = dfInventory['fprice'].apply(lambda x: 0 if pd.isnull(x) else float(x))
        dfInventory['fprice'] = dfInventory.apply(lambda x: x['fprice'] if x['fprice'] else float(x['cost'])*1.3, axis=1)
        dfInventory['date_id'] = self.dEnd

        Inventory.objects.filter(date__calendaryear=self.iYear, date__accountingmonth=self.iMonth).delete()
        odo(dfInventory, ENGINE.CONSTR_SCMSDW + "::YY_INVENTORY")


class EtlIcbcGcard(EtlBase):
    def __init__(self, iYear=None, iMonth=None):
        super(EtlIcbcGcard, self).__init__(iYear, iMonth)
        self.dNow = datetime.datetime.now()
        self.dDate = datetime.date.today()

    def getRequest(self):
        sDate = datetime.date.strftime(self.dNow, '%Y%m%d')
        SendTime = datetime.date.strftime(self.dNow, '%Y%m%d%H%M%S%f')[:-3]
        sTime = datetime.date.strftime(self.dNow, '%H%M%S%f')[:-3]
        sSerial = datetime.date.strftime(self.dNow, '%Y%m%d%f') + '0'
        sCerID = "NFQS.y.1001"

        sTransCode = 'QSKGJDZGX'
        sCIS = '100190004964204'
        iBankCode = 102
        sAccNo = '1001794319300108055'

        url = "http://192.168.81.10:448/servlet/ICBCCMPAPIReqServlet"
        params = {'userID': sCerID, 'PackageID': sSerial, 'SendTime': SendTime}
        headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        post = 'Version=0.0.1.0&TransCode={sTransCode}&BankCode={iBankCode}&GroupCIS={sCIS}\
        &ID={sCerID}&PackageID={sSerial}&Cert=&reqData='
        xml = """
        <?xml version="1.0" encoding = "GBK"?>
            <CMS>
                <eb>
                    <pub>
                        <TransCode>{sTransCode}</TransCode>
                        <CIS>{sCIS}</CIS>
                        <BankCode>{iBankCode}</BankCode>
                        <ID>{sCerID}</ID>
                        <TranDate>{sDate}</TranDate>
                        <TranTime>{sTime}</TranTime>
                        <fSeqno>{sSerial}</fSeqno>
                    </pub>
                    <in>
                        <AcctNo>{sAccNo}</AcctNo>
                        <Status>2</Status>
                        <NextTag></NextTag>
                    </in>
                </eb>
            </CMS>
        """
        content = (post + xml).format(sTransCode=sTransCode, sCIS=sCIS, iBankCode=iBankCode,
                                      sCerID=sCerID, sDate=sDate, sTime=sTime,
                                      sSerial=sSerial, sAccNo=sAccNo)
        req = requests.Request(method='post', url=url, params=params, headers=headers, data=content)
        return req

    def getResponse(self):
        req = self.getRequest()
        with requests.Session() as s:
            prepped = s.prepare_request(req)
            resp = s.send(prepped)
        return resp

    def getsResp(self):
        resp = self.getResponse()
        sContent = urllib.parse.parse_qs(resp.text)['reqData'][0]
        bText = base64.b64decode(sContent.replace(' ', '+'))
        sText = bText.decode('gbk')
        return sText

    def getdData(self):
        sText = self.getsResp()
        dResp = xmltodict.parse(sText)
        try:
            dRd = dResp['CMS']['eb']['out']['rd']
            dRd = dRd if isinstance(dRd, list) else [dRd]
        except KeyError:
            return None
        return dRd

    def getdfData(self):
        dRd = self.getdData()
        if dRd is None:
            return None
        dfSlip=pd.DataFrame.from_dict(dRd)
        dfSlip.columns = map(str.lower, dfSlip.columns)
        return dfSlip

    def sync(self):
        oResource = IcbcGcardResource()
        dfGcard = self.getdfData()
        if dfGcard is not None:
            dGcard = dfGcard.to_dict("split")
            lt = [tuple(x) for x in dGcard["data"]]
            columnsen = [x.lower() if x != 'gcardno' else 'id' for x in dGcard["columns"]]
            columnscn = [IcbcGcard._meta.get_field_by_name(x)[0].verbose_name for x in columnsen]
            dataset = tablib.Dataset(*lt, headers=columnscn)
            result = oResource.import_data(dataset, dry_run=False, raise_error=True)
            if result.has_errors():
                for r in result.rows:
                    print(r.errors.pop().traceback)


class EtlIcbcSlip(EtlBase):
    def __init__(self, iYear=None, iMonth=None):
        super(EtlIcbcSlip, self).__init__(iYear, iMonth)
        self.dNow = datetime.datetime.now()
        self.dDate = datetime.date.today()

    def getRequest(self):
        SendTime = datetime.date.strftime(self.dNow, '%Y%m%d%H%M%S%f')[:-3]
        sTime = datetime.date.strftime(self.dNow, '%H%M%S%f')[:-3]
        sSerial = datetime.date.strftime(self.dNow, '%Y%m%d%f') + '0'
        sCerID = "NFQS.y.1001"

        sTransCode = 'QHISD'
        sCIS = '100190004964204'
        iBankCode = 102
        sAccNo = '1001794319300108055'

        sStartDate = datetime.date.strftime(self.dDate + datetime.timedelta(0), '%Y%m%d')
        sEndData = datetime.date.strftime(self.dDate + datetime.timedelta(0), '%Y%m%d')

        url = "http://192.168.81.10:448/servlet/ICBCCMPAPIReqServlet"
        params = {'userID': sCerID, 'PackageID': sSerial, 'SendTime': SendTime}
        headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        post = 'Version=0.0.1.0&TransCode={sTransCode}&BankCode={iBankCode}&GroupCIS={sCIS}\
        &ID={sCerID}&PackageID={sSerial}&Cert=&reqData='
        xml = """
        <?xml version="1.0" encoding = "GBK"?>
            <CMS>
                <eb>
                    <pub>
                        <TransCode>{sTransCode}</TransCode>
                        <CIS>{sCIS}</CIS>
                        <BankCode>{iBankCode}</BankCode>
                        <ID>{sCerID}</ID>
                        <TranDate>{sEndData}</TranDate>
                        <TranTime>{sTime}</TranTime>
                        <fSeqno>{sSerial}</fSeqno>
                    </pub>
                    <in>
                        <AccNo>{sAccNo}</AccNo>
                        <BeginDate>{sStartDate}</BeginDate>
                        <EndDate>{sEndData}</EndDate>
                        <MinAmt>0</MinAmt>
                        <MaxAmt>10000000000</MaxAmt>
                        <BankType></BankType>
                        <NextTag></NextTag>
                        <CurrType></CurrType>
                        <DueBillNo></DueBillNo>
                        <AcctSeq></AcctSeq>
                    </in>
                </eb>
            </CMS>
        """
        content = (post + xml).format(sTransCode=sTransCode, sCIS=sCIS, iBankCode=iBankCode,
        sCerID=sCerID, sStartDate=sStartDate, sEndData=sEndData, sTime=sTime,
        sSerial=sSerial, sAccNo=sAccNo)
        req = requests.Request(method='post', url=url, params=params, headers=headers, data=content)
        return req

    def getResponse(self):
        req = self.getRequest()
        with requests.Session() as s:
            prepped = s.prepare_request(req)
            resp = s.send(prepped)
        return resp

    def getsResp(self):
        resp = self.getResponse()
        sContent = urllib.parse.parse_qs(resp.text)['reqData'][0]
        bText = base64.b64decode(sContent.replace(' ', '+'))
        sText = bText.decode('gbk')
        return sText

    def getdData(self):
        sText = self.getsResp()
        dResp = xmltodict.parse(sText)
        try:
            dRd = dResp['CMS']['eb']['out']['rd']
            dRd = dRd if isinstance(dRd, list) else [dRd]
        except KeyError:
            return None
        return dRd

    def getdfData(self):
        dRd = self.getdData()
        if dRd is None:
            return None
        dfSlip=pd.DataFrame.from_dict(dRd)
        dfSlip.columns = map(str.lower, dfSlip.columns)
        dfSlip.rename(columns={"date": "date_id"}, inplace=True)
        dfSlip['recipaccno'].fillna('none', inplace=True)
        dfSlip.loc[:, 'id'] = dfSlip.apply(lambda x: x['recipaccno'] + '-' + x['onlysequence'], axis=1)
        # dfSlip.drop('time', axis=1, inplace=True)
        dfSlip.loc[:, 'date_id'] = dfSlip['date_id'].apply(lambda x: datetime.date(year=int(x[0:4]), month=int(x[4:6]), day=int(x[6:8])))
        return dfSlip

    def sync(self):
        oResource = IcbcSlipResource()
        dfSlip = self.getdfData()
        if dfSlip is not None:
            lColumn = list({c.attname for c in IcbcSlip._meta.get_fields()} & set(dfSlip.columns))
            dSlip = dfSlip[lColumn].to_dict("split")
            lt = [tuple(x) for x in dSlip["data"]]
            columnsen = [x.lower() if x!='Toutfo' else 'id' for x in dSlip["columns"]]
            columnscn = [IcbcSlip._meta.get_field(x).verbose_name for x in columnsen]
            dataset = tablib.Dataset(*lt, headers=columnscn)
            result = oResource.import_data(dataset, dry_run=False, raise_error=True)
            if result.has_errors():
                for r in result.rows:
                    print(r.errors.pop().traceback)


class EtlIcbcGslip(EtlBase):
    def __init__(self, iYear=None, iMonth=None):
        super(EtlIcbcGslip, self).__init__(iYear, iMonth)
        self.dNow = datetime.datetime.now()
        self.dDate = datetime.datetime.now()

    def getRequest(self):
        sDate = datetime.date.strftime(self.dNow, '%Y%m%d')
        sData = datetime.date.strftime(self.dDate, '%Y%m%d')
        SendTime = datetime.date.strftime(self.dNow, '%Y%m%d%H%M%S%f')[:-3]
        sTime = datetime.date.strftime(self.dNow, '%H%M%S%f')[:-3]
        sSerial = datetime.date.strftime(self.dNow, '%Y%m%d%f') + '0'
        sCerID = "NFQS.y.1001"

        sTransCode = 'QRYGJDTL'
        sCIS = '100190004964204'
        iBankCode = 102
        sAccNo = '1001794319300108055'

        sStartDate = datetime.date.strftime(self.dDate + datetime.timedelta(0), '%Y%m%d')
        sEndDate = datetime.date.strftime(self.dDate + datetime.timedelta(0), '%Y%m%d')

        url = "http://192.168.81.10:448/servlet/ICBCCMPAPIReqServlet"
        params = {'userID': sCerID, 'PackageID': sSerial, 'SendTime': SendTime}
        headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        post = 'Version=0.0.1.0&TransCode={sTransCode}&BankCode={iBankCode}&GroupCIS={sCIS}\
        &ID={sCerID}&PackageID={sSerial}&Cert=&reqData='
        xml = """
        <?xml version="1.0" encoding = "GBK"?>
            <CMS>
                <eb>
                    <pub>
                        <TransCode>{sTransCode}</TransCode>
                        <CIS>{sCIS}</CIS>
                        <BankCode>{iBankCode}</BankCode>
                        <ID>{sCerID}</ID>
                        <TranDate>{sDate}</TranDate>
                        <TranTime>{sTime}</TranTime>
                        <fSeqno>{sSerial}</fSeqno>
                    </pub>
                    <in>
                        <AcctNo>{sAccNo}</AcctNo>
                        <StartDate>{sStartDate}</StartDate>
                        <EndDate>{sEndDate}</EndDate>
                        <NextTag></NextTag>
                        <AcctSeq></AcctSeq>
                        <CardNo></CardNo>
                    </in>
                </eb>
            </CMS>
        """
        content = (post + xml).format(sTransCode=sTransCode, sCIS=sCIS, iBankCode=iBankCode,
                                      sCerID=sCerID, sStartDate=sStartDate, sEndDate=sEndDate, sDate=sDate, sTime=sTime,
                                      sSerial=sSerial, sAccNo=sAccNo)
        req = requests.Request(method='post', url=url, params=params, headers=headers, data=content)
        return req

    def getResponse(self):
        req = self.getRequest()
        with requests.Session() as s:
            prepped = s.prepare_request(req)
            resp = s.send(prepped)
        return resp

    def getsResp(self):
        resp = self.getResponse()
        sContent = urllib.parse.parse_qs(resp.text)['reqData'][0]
        bText = base64.b64decode(sContent.replace(' ','+'))
        sText = bText.decode('gbk')
        return sText

    def getdData(self):
        sText = self.getsResp()
        dResp = xmltodict.parse(sText)
        try:
            dRd = dResp['CMS']['eb']['out']['rd']
            dRd = dRd if isinstance(dRd, list) else [dRd]
        except KeyError:
            return None
        return dRd

    def getdfData(self):
        dRd = self.getdData()
        if dRd is None:
            return None
        dfSlip=pd.DataFrame.from_dict(dRd)
        dfSlip.columns = map(str.lower, dfSlip.columns)
        dfSlip['rciacctno'].fillna('none', inplace=True)
        dfSlip.loc[:, 'id'] = dfSlip['rciacctno'] + '-' + dfSlip['transerialno']
        dfSlip.loc[:, 'date_id'] = dfSlip['worddate'].apply(lambda x: datetime.date(year=int(x[0:4]),
                                                                                    month=int(x[4:6]), day=int(x[6:8])))
        return dfSlip

    def sync(self):
        oResource = IcbcGslipResource()
        dfSlip = self.getdfData()
        if dfSlip is not None:
            lColumn = [c.attname for c in IcbcGslip._meta.get_fields()]
            # lColumn.remove('date')
            dSlip = dfSlip[lColumn].to_dict("split")
            lt = [tuple(x) for x in dSlip["data"]]
            columnsen = [x.lower() for x in dSlip["columns"]]
            columnscn = [IcbcGslip._meta.get_field(x).verbose_name for x in columnsen]
            dataset = tablib.Dataset(*lt, headers=columnscn)
            result = oResource.import_data(dataset, dry_run=False, raise_error=True)
            if result.has_errors():
                for r in result.rows:
                    print(r.errors.pop().traceback)


class OpEtlYy(OpEtl):
    def initEtlClass(self, sEtlClass, iYear, iMonth):
        EtlClass = getattr(self.mModule, sEtlClass)
        try:
            opc = EtlClass(self.iAccid, iYear, iMonth)
        except (OperationalError, ParameterError):
            print('init OpClass {}.{}.{}.{} fail.'.format(sEtlClass, str(self.iAccid), str(iYear), str(iMonth)))
            opc = None
        return opc

    def syncDefine(self):
        for oDBranch in Dict.objects.filter(syscode='accid', string01='ETL'):
            for oDAccid in oDBranch.get_descendants():
                self.iAccid = oDAccid.value
                super(OpEtlYy, self).syncDefine()

    def syncSingle(self, sClass):
        for oDBranch in Dict.objects.filter(syscode='accid', string01='ETL'):
            for oDAccid in oDBranch.get_descendants():
                self.iAccid = oDAccid.value
                super(OpEtlYy, self).syncSingle(sClass)

    def syncRecentSingle(self, sClass, iMonths):
        for oDBranch in Dict.objects.filter(syscode='accid', string01='ETL'):
            for oDAccid in oDBranch.get_descendants():
                self.iAccid = oDAccid.value
                super(OpEtlYy, self).syncRecentSingle(sClass, iMonths)

    def syncData(self):
        for oDBranch in Dict.objects.filter(syscode='accid', string01='ETL'):
            for oDAccid in oDBranch.get_descendants():
                self.iAccid = oDAccid.value
                super(OpEtlYy, self).syncData()

    def syncRecentData(self, iMonths):
        for oDBranch in Dict.objects.filter(syscode='accid', string01='ETL'):
            for oDAccid in oDBranch.get_descendants():
                self.iAccid = oDAccid.value
                super(OpEtlYy, self).syncRecentData(iMonths)


class EtlAcctBook(EtlYy):
    """

    """
    def sync(self):
        # acctbook in 2010 is incomplete.
        AcctBook.objects.filter(code__accid__syscode='accid', code__accid__value=self.iAccid,
                                code__year_id=self.iYear, period=self.iMonth).delete()

        dfAcctBook = pd.read_sql("""SELECT NULL id, a.id i_id, b.subjectcode code_id, a.acctid, b.acctname,
        CASE WHEN a.accttype = 0 THEN '现金' WHEN a.accttype = 1 THEN '银行' END accttype,
        a.acctdate date_id, a.voucherstr, a.vouchernum, a.period, a.debit, a.credit, a.summary
        FROM CN_AcctBook a, CN_AcctInfo b WHERE a.AcctID = b.ID AND YEAR(a.acctdate) = {} AND a.period = {}
        ORDER BY a.acctdate, a.id""".format(self.iYear, self.iMonth), ENGINE.UFIDA(self.iAccid, self.iYear))
        dfAcctBook['code_id'] = dfAcctBook['code_id'].apply(self.getCodeId)

        dfBegin = AnaAcctBook(self.iAccid, self.iYear, self.iMonth).getdfBeginAcctBook()

        # dfAcctBook = pd.concat([dfBegin, dfAcctBook])
        dfAcctBook.reset_index(drop=True, inplace=True)
        if len(dfAcctBook) > 0:
            dfAcctBook['amount'] = dfAcctBook.apply(lambda x: x.debit - x.credit, axis=1)
            dfAcctBook['cumsum'] = dfAcctBook.groupby('acctid')['amount'].cumsum()
            dfAcctBook['cumsum'] = dfAcctBook.apply(lambda x: x['cumsum'] + \
            dfBegin[dfBegin['acctid'] == x['acctid']].iloc[0]['amountend'], axis=1)
            dfAcctBook['amountend'] = dfAcctBook['cumsum']
            dfAcctBook.drop(['amount', 'cumsum'], axis=1, inplace=True)

        odo(dfAcctBook, ENGINE.CONSTR_SCMSDW + '::YY_ACCTBOOK')


class EtlAcctBookFull(EtlYy):
    def sync(self):
        aab = AnaAcctBook(self.iAccid, self.iYear, self.iMonth)
        dfAcctBookFull = aab.getdfAcctBookFull()
        dfAcctBookFull['id'] = None
        AcctBookFull.objects.filter(code__accid__syscode='accid', code__accid__value=self.iAccid,
                                    date__calendaryear=self.iYear, date__accountingmonth=self.iMonth).delete()
        odo(dfAcctBookFull, ENGINE.CONSTR_SCMSDW + "::YY_ACCTBOOKFULL")


class EtlShortBorrowFull(EtlYy):
    def sync(self):
        asb = AnaShortBorrow(self.iAccid, self.iYear, self.iMonth)
        dfShortBorrowFull = asb.getdfShortBorrowFull()
        dfShortBorrowFull['id'] = None
        ShortBorrowFull.objects.filter(code__accid__syscode='accid', code__accid__value=self.iAccid,
                                    date__calendaryear=self.iYear, date__accountingmonth=self.iMonth).delete()
        odo(dfShortBorrowFull, ENGINE.CONSTR_SCMSDW + "::YY_SHORTBORROWFULL")


class EtlPayAge(EtlYy):
    def sync(self):
        ara = AnaAge(self.iAccid, self.iYear, self.iMonth)
        dfPayAge = ara.getdfPayAge()
        dfPayAge.rename(columns={"date__id": "date_id"}, inplace=True)
        dfPayAge['id'] = None
        PayAge.objects.filter(vendor__accid__value=self.iAccid, date__calendaryear=self.iYear, date__accountingmonth=self.iMonth).delete()
        odo(dfPayAge, ENGINE.CONSTR_SCMSDW + "::YY_PAYAGE")


