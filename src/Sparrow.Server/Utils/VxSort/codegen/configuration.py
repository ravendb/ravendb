

class Configuration:

    def __init__(self):
        self.max_bitonic_sort_vectors = 20
        self.unroll_bitonic_sorters = 22
        self.unroll = 8
        self.pack_unroll = 8
        self.do_prefetch = False
        self.is_debug = False
        self.can_pack = True
        self.namespace = "Sparrow.Server.Utils.VxSort"


