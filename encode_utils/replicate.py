
class ExpReplicates():

    def __init__(self, conn, experiment_id):
        """
        Args:
            conn: `encode_utils.connection.Connection` instance.
            experiment_id: `str`. An identifier for an experiment record on the ENCODE Portal. 
        """
        self.conn = conn
        exp = self.conn.get(rec_ids=experiment_id, ignore404=False)
        self.reps = exp["replicates"]
        self.rep_hash = self._get_rep_hash()

    def _get_rep_hash(self):
        """
        Creates a hierarchicl representation of the replicates on the experiment, categorizing them
        first by the asssociated biosample and then by the associted library. This is stored in a 
        `dict` of the format

          {"$biosample_accession": {
              "$library_accession": $rep
            } 
          }

        where $biosample_accession is the value of a biosample record's accession property,
        $library_accession is the value of a library record's accession property, and $rep is the
        associated replicate record (serialized in JSON).

        This function assumes that there is only one replicate object per library.

        Args:
            rec_id: `str`. An identifier for the record on the Portal.
        """
        res = {}
        for rep in self.reps:
            library_acc = rep["library"]["accession"]
            biosample_acc = rep["library"]["biosample"]["accession"]
            brn = rep["biological_replicate_number"]
            trn = rep["technical_replicate_number"]
            if biosample_acc not in res:
                res[biosample_acc] = {}
            if library_acc not in res[biosample_acc]:
                res[biosample_acc][library_acc] = {"brn": brn, "trn": trn}
        return res

    def does_rep_exist(self, biosample_accession, library_accession):
        """
        Checks whether the experiment contains a replicate already for the given biosample and library
        records. Useful for clients trying to determine whether creating a new replicate is necessary
        or not when submitting FASTQ files. 

        Returns: 
            `False`: A replicate does not exist.
            `dict`: The replicate JSON if there is a replicate. 
        """
        if not biosample_accession in self.rep_hash:
            return False
        if not library_accession in self.rep_hash[biosample_accession] :
            return False
        return self.rep_hash[biosample_accession][library_accession]

    def does_brn_exist(self, brn):
        """
        Checks self.rep_hash to see if there is a biological replicate with the given replicate number.
   
        Returns:
            `bool`.
        """
        for bio_acc in self.rep_hash:
            for lib_acc in self.rep_hash[bio_acc]:
                rep = self.rep_hash[bio_acc][lib_acc]
                if brn == rep["brn"]:
                    return True
        return False

    def does_trn_exist(self, brn, trn):
        """
        Checks self.rep_hash to see if there is a replicate object that exists with the given 
        biosample_replicate_number and technical_replicate_number.
   
        Returns:
            `bool`.
        """
        for bio_acc in self.rep_hash:
            for lib_acc in self.rep_hash[bio_acc]:
                rep = self.rep_hash[bio_acc][lib_acc]
                if brn != rep["brn"]:
                    break
                if trn == rep["trn"]:
                    return True
        return False
            

    
