#include <stdlib.h>
#include <stdio.h>
#include <string>
#include <string.h>
#include <sstream>
#include <errno.h>
#include <assert.h>
#include <stdint.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <inttypes.h>
#include <vector>
#include <algorithm>

#include "recombine.h"


// split string up on a token boundary
void split(const std::string& s, char c, std::vector<std::string>& v) {
	std::string::size_type i = 0;
	std::string::size_type j = s.find(c);

	while (j != std::string::npos) {
		v.push_back(s.substr(i, j-i));
		i = ++j;
		j = s.find(c, j);
		if (j == std::string::npos)
			v.push_back(s.substr(i, s.length()));
	}
}


void print_usage() {
    printf("Usage: recombine -f <file list> || -g <input file list> -o <obsid> -t <secondid> -i <output dir> -d <course channel freq list> -c <skip course chan> -s <skip ICS> \n");
}


int main(int argc, char **argv) {

	volatile bool sflag, cflag = false;
	char *fvalue = NULL;
	char *ovalue = NULL;
	char *ivalue = NULL;
	char *gvalue = NULL;
	char *dvalue = NULL;
	char *tvalue = NULL;
	int index;
	int c;

	opterr = 0;

	while ((c = getopt(argc, argv, "csg:f:o:i:d:t:")) != -1)
		switch (c)
		{
			case 'c':
				cflag = true;
				break;
			case 's':
				sflag = true;
				break;
			case 'd':
				dvalue = optarg;
				break;
			case 'g':
				gvalue = optarg;
				break;
			case 'f':
				fvalue = optarg;
				break;
			case 'o':
				ovalue = optarg;
				break;
			case 'i':
				ivalue = optarg;
				break;
			case 't':
				tvalue = optarg;
				break;
			case '?':
				 if (optopt == 'f')
					 fprintf (stderr, "Option -f list of input filenames no more than 32 files e.g. \"file1.dat file2.dat\".\n");
				 else if (optopt == 'g')
					 fprintf (stderr, "Option -g file containing list of input filenames (one filename per line no more that 32 files).\n");
				 else if (optopt == 'o')
					 fprintf (stderr, "Option -o observation ID of the set.\n");
				 else if (optopt == 'i')
					 fprintf (stderr, "Option -i output directory containing 24 course channel files.\n");
				 else if (optopt == 'd')
					 fprintf (stderr, "Option -d course channel list (24 values comma separated).\n");
				 else if (optopt == 't')
					 fprintf (stderr, "Option -t must specify second ID.\n");
				 else if (isprint (optopt))
					 fprintf (stderr, "Unknown option.\n");
				 else
				   fprintf (stderr,"Unknown option character.\n");

				 return EXIT_FAILURE;

			default:
				print_usage();
				exit(EXIT_FAILURE);
		}

	if (ovalue == NULL) {
		printf("Invalid command line, observation ID not specified\n");
		print_usage();
		exit(EXIT_FAILURE);
	}

	if (ivalue == NULL) {
		printf("Invalid command line, output directory not specified\n");
		print_usage();
		exit(EXIT_FAILURE);
	}

	if (tvalue == NULL) {
		printf("Invalid command line, second ID not specified\n");
		print_usage();
		exit(EXIT_FAILURE);
	}

	if (fvalue == NULL && gvalue == NULL) {
		printf("Invalid command line, list of input files or a file containing filenames not specified\n");
		print_usage();
		exit(EXIT_FAILURE);
	}

	if (fvalue != NULL && gvalue != NULL) {
		printf("Invalid command line, can not specify both list of input files or a file containing filenames\n");
		print_usage();
		exit(EXIT_FAILURE);
	}

	if (dvalue == NULL) {
		printf("Invalid command line, must specify course channel list\n");
		print_usage();
		exit(EXIT_FAILURE);
	}

	course_chan_freq in, out;
	unsigned int course_swap_index = 0;

	std::string cmdinput = std::string(dvalue);
	std::vector<std::string> v;
	split(cmdinput, ',', v);

	if (v.size() != 24) {
		printf("Invalid command line, must specify 24 course channel frequencies\n");
		print_usage();
		exit(EXIT_FAILURE);
	}

	// convert strings to integers
	unsigned int x;
	for (int i = 0; i < v.size(); i++) {
		std::stringstream str(v[i]);
		str >> x;
		in.m_freq[i] = x;
	}

	// check for duplicate frequency entries
	for (int i = 0; i < 24; i++) {
		for (int j = i + 1; j < 24; j++) {
			if (in.m_freq[i] == in.m_freq[j]) {
				printf("Duplicate course channel frequency entry found.\n");
				exit(EXIT_FAILURE);
			}
		}
	}

	// check if we need to swap channels; if so swap them.
	course_channel_swap(&in, &out, &course_swap_index);

	course_chan_input_array input;
	course_chan_output_array output;
	ics_handle ics_handle;

	memset(&input, 0, sizeof(course_chan_input_array));
	memset(&output, 0, sizeof(course_chan_output_array));
	memset(&ics_handle, 0, sizeof(ics_handle));

	int ret = 0;

	if (fvalue != NULL) {
		// each file must be separated by a white space
		std::string fileinput = std::string(fvalue);
		std::vector<std::string> fv;
		split(fileinput, ' ', fv);

		if (fv.size() != 32) {
			printf("Invalid command line, must specify 32 input files.\n");
			print_usage();
			exit(EXIT_FAILURE);
		}

		for (int i = 0; i < fv.size(); i++) {
			strcpy(input.m_handles[i].m_id, fv[i].c_str());
		}

		if (open_input_from_file(&input) != 0) {
			printf("%s\n", strerror(errno));
			return EXIT_FAILURE;
		}

	}
	else if (gvalue != NULL) {
		ret = open_input_from_file_list(gvalue, &input);
		if (ret != 0) {
			printf("%s\n", strerror(errno));
			return EXIT_FAILURE;
		}
	}
	else {
		printf("Input not defined!\n");
		return EXIT_FAILURE;
	}

	for (int i = 0; i < 32; ++i)
		if (input.m_handles[i].pad_input == true)
			printf("Warning: stream %s either failed to open or does not exist, input will be padded with zeros!\n", input.m_handles[i].m_id);

	// ensure output directory exists; if not create it
	struct stat st = {0};
	if (stat(ivalue, &st) == -1)
	    mkdir(ivalue, 0755);

	std::string out_path = std::string(ivalue);
	for (int i = 0; i < 24; ++i) {
		std::stringstream s;
		s << out.m_freq[i];
		std::string full_out_path = out_path + "/" + std::string(ovalue) + "_" + std::string(tvalue) + "_ch" + s.str() + ".dat";
		strcpy(output.m_handles[i].m_id, full_out_path.c_str());
	}

	if (!sflag) {
		// open output for ics
		std::string full_out_path_ics = out_path + "/" + std::string(ovalue) + "_" + std::string(tvalue) + "_ics.dat";
		if ((ics_handle.m_handle = open(full_out_path_ics.c_str(), O_WRONLY | O_CREAT | O_TRUNC, S_IRWXU)) < 0)
			return EXIT_FAILURE;
	}

	if (!cflag) {
		ret = open_output_to_file(&output);
		if (ret != 0) {
			printf("%s\n", strerror(errno));
			return EXIT_FAILURE;
		}
	}

	ret = recombine(&input, &output, &ics_handle, course_swap_index, sflag, cflag);
	if (ret != 0) {
		printf("%s\n", strerror(errno));
		return EXIT_FAILURE;
	}

	close_input_handles(&input);

	if (!cflag)
		close_output_handles(&output);

	if (!sflag)
		close(ics_handle.m_handle);

	return 0;

}
