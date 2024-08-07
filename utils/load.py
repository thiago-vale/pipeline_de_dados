import logging

class Load():

    def __init__(self):
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)

    def csv(self,df, mode, path):

        if mode == 'incremen':

            df.write.format("csv").mode('append').save(path)
        
        elif mode == 'full':

            df.write.format("csv").mode('overwrite').save(path)

        else:

            self.logger.info("mode not found")

        return None
    
    def parquet(self, df, mode, path):

        if mode == 'incremen':

            df.write.format("parquet").mode('append').save(path)
        
        elif mode == 'full':

            df.write.format("parquet").mode('overwrite').save(path)

        else:
            
            self.logger.info("mode not found")

        return None
    
    def delta(self, df, mode, path):

        if mode == 'incremen':

            df.write.format("delta").mode('append').save(path)
        
        elif mode == 'full':

            df.write.format("delta").mode('overwrite').save(path)

        else:
            
            self.logger.info("mode not found")

        return None