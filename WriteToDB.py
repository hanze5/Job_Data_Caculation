


def WrToDB(df,tarTable,mode):
    df.write.format('jdbc').options(
        url='jdbc:mysql://47.113.123.159/job_res',
        driver='com.mysql.jdbc.Driver',
        dbtable=tarTable,
        user='pa',
        password='258258cqu').mode(mode).save()
