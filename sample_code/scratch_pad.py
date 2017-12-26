__author__ = 'pbhandari'


import traceback



class InterestingMRNs(object):

    def __init__(self):
        print 'a'


    def create_cti_interesting_mrns(self):


        try:

            print'create_cti_interesting_mrns'

        except:

            print "exception encountered.."


    def insert_into_cti_interesting_mrns(self):

        """
        We are interested charges that meet the following conditions
            1. The MRN has been viewed by the customer before
            2. All the missing_master lines that have been posted after the first view date of the MRN
                (Realize that the first view date could be years before the actual charge we are talking about. This is a rough
                swag of the charges that we are interested in evaluating)

        :return:
        """

        sql = """
                        insert into cti_interesting_mrns
                          (organization_name,
                             mrn,
                             mcj_id,
                             mcj_service_date,
                             description                    ,
                             status                         )
                          select distinct mcj.organization_name
                            , mcj.mrn
                            , mcj.id mcj_id
                            , mcj.service_date service_date
                            , mcj.description
                            , mcj.status
                          from missing_charges_jess mcj
                            join audit a1 on (mcj.id = a1.missingchargeid)
                            join users u on (a1.username = u.username
                                                            and a1.username not in ('a_falco', 'threatx' )
                                                            and (u.staff is FALSE
                                                                 or ((a1.type = 'HOSPITALEXPORT' or a1.type = 'POEXPORT'
                                                                 or (a1.type = 'CHARGE' ))
                                                                     and u.staff is true))
                                                             )
                           where mcj.organization_name = %(org_name)s
                      """

        try:
            print "Executing insert_into_cti_interesting_mrns"
            cur = self.conn.cursor()
            query = cur.mogrify(sql, {'org_name':self.org_name})
            logging.info(query)
            cur.execute(query)
            inserted_accts = cur.rowcount
            print "Insert " + str(inserted_accts) + " Instances "

        except:
            print "exception encountered.. insert_into_cti_interesting_mrns"
            logging.error(traceback.format_exc())
            exit(1)
        self.conn.commit()

    def analyze_table(self):
        """

        :return:
        """
        sql = "analyze cti_interesting_mrns"

        try:
            curs = self.conn.cursor()
            curs.execute(sql)

        except:
            logging.error("analyze cti_interesting_mrns...")
            logging.error(traceback.format_exc())
            exit(1)


    def teardown(self):
        """

        :return:
        """

        sql = "delete from acustream.cti_interesting_mrns where organization_name = %(org_name)s"

        try:
            print "Reseting cti_interesting_mrns"
            cur = self.conn.cursor()
            query = cur.mogrify(sql, {'org_name':self.org_name})
            logging.info(query)
            cur.execute(query)
            inserted_accts = cur.rowcount
            print "Deleted " + str(inserted_accts) + " Instances "

        except:
            logging.error(traceback.format_exc())
            exit(1)

        self.conn.commit()


    def main(self):
        """

        :return:
        """
        print "Start create_cti_interesting_mrns"
        imrn = InterestingMRNs(self.org_name, self.params, self.conn, self.trans_post_date)
        imrn.create_cti_interesting_mrns()
        imrn.teardown()
        imrn.insert_into_cti_interesting_mrns()
        imrn.analyze_table()
        print "create_cti_interesting_mrns Completed"

if __name__ == '__main__':


    print 'test'
    mrn = InterestingMRNs()
    mrn.main()