
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

          {
              "$biosample_accession": {
                  "brn": $num,
                  "libraries": {
                      "$library_accession": {
                          "trn": $num,
                          "record": "replicate_json"
                      }
                  }
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
                res[biosample_acc] = {"brn": brn, "libraries": {}}
            res[biosample_acc]["libraries"][library_acc] = {
                "trn": trn,
                "replicate_json": rep
            }
        return res

    def get_rep(self, biosample_accession, library_accession):
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
        if not library_accession in self.rep_hash[biosample_accession]["libraries"]:
            return False
        return self.rep_hash[biosample_accession]["libraries"][library_accession]["replicate_json"]

    def get_tech_rep_hash(self, brn):
        """
        Gives the value of `self.rep_hash[brn]["libraries"]` if brn exists.

        Returns:
            `dict`: Empty if brn doesn't exist, otherwise the value of self.rep_hash[brn]["libraries"].
        """
        for bio_acc in self.rep_hash:
            if brn == self.rep_hash[bio_acc]["brn"]:
                return self.rep_hash[bio_acc]["libraries"]
        return {}

    def does_brn_exist(self, brn):
        """
        Checks self.rep_hash to see if there is a biological replicate with the given
        biosample_replicate_number.
        """
        if self.get_tech_rep_hash(brn):
            return True
        return False

    def does_trn_exist(self, brn, trn):
        """
        Checks self.rep_hash to see if there is a replicate object that exists with the given
        biosample_replicate_number and technical_replicate_number.

        Returns:
            `bool`.
        """
        lib_hash = self.get_tech_rep_hash(brn)
        if lib_hash:
            for lib_acc in lib_hash:
                if trn == lib_hash[lib_acc]["trn"]:
                    return True
        return False

    def suggest_brn(self):
        """
        Select a biosample_replicate_number (brn) that is one greater than the number of
        existing biosamples on the experiment.  If that number is already in use, increment until
        it is unique.
        """
        brn = len(self.rep_hash) + 1
        while self.does_brn_exist(brn):
            brn += 1
        return brn

    def suggest_trn(self, biosample_accession):
        """
        For technical_replicate_number (trn), use a number that is one greater than the number of
        existing technical replicates on the experiment for the given biosample. If the given
        biosample isn't yet registered as a replicate, set trn to 1.
        """
        if biosample_accession not in self.rep_hash:
            return 1
        else:
            brn = self.rep_hash[biosample_accession]["brn"]
            trn = len(self.rep_hash[biosample_accession]["libraries"]) + 1
            while self.does_trn_exist(brn=brn, trn=trn):
                trn += 1
        return trn

    def suggest_brn_trn(self, biosample_accession):
        return [self.suggest_brn(), self.suggest_trn(biosample_accession)]


