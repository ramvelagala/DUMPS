import logging.config

def something():
    print("ok")
    # logging.config.fileConfig("writelog.txt")
    # logging.config.fileConfig("write.log", defaults=None)
    logger = logging.getLogger(__name__)
    f_handler = logging.FileHandler("profile.log", 'w')
    f_handler.setLevel(10)
    f_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    f_handler.setFormatter(f_format)
    logger.addHandler(f_handler)
    logger.debug("Helllllllllllllllllllllllllllllllllllo")
    # logging.getLogger("write.log")
    # logging.basicConfig(level=logging.debug, filemode='w', filename="writelog.txt")
    # logging.getLogger(__name__)

    logger.debug("this is debug")
    # logging.info("This is from sample")



something()
