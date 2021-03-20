class Processor_base {

  public:
  std::string line_, volumeId_;
  std::filebuf fb_;
  std::istream* is_;

  void openVolumeFile(const char* file) {
    if (!fb_.open(argv[2], std::ios::in)) {
      std::cerr << "Input file error: " << argv[2] << std::endl;
      exit(1);
    }
    is_ = new std::istream(&fb_);
  }
  
}
