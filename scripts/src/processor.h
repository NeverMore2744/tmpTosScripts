#include<string>
#include<fstream>
#include<iostream>
#include<vector>


class Processor_base {

  public:
  std::string line_, volumeId_;
  std::filebuf fb_;
  std::istream* is_;
  std::vector<std::string> filenames_;

  void openVolumeFile(const char* file) {
    if (!fb_.open(file, std::ios::in)) {
      std::cerr << "Input file error: " << file << std::endl;
      exit(1);
    }
    is_ = new std::istream(&fb_);
  }

  void outputFileNames() {
    for (auto& it : filenames_) {
      std::cout << it << std::endl;
    }
  }


  
};
