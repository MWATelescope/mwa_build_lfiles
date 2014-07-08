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
#include <dirent.h>

#include <netdb.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include "recombine.h"


static unsigned short byte_to_sum[256] = {
0,1,4,9,16,25,36,49,49,49,36,25,16,9,4,1,1,2,5,10,17,26,37,50,50,50,37,26,17,10,5,2,
4,5,8,13,20,29,40,53,53,53,40,29,20,13,8,5,9,10,13,18,25,34,45,58,58,58,45,34,25,18,13,10,
16,17,20,25,32,41,52,65,65,65,52,41,32,25,20,17,25,26,29,34,41,50,61,74,74,74,61,50,41,34,29,26,
36,37,40,45,52,61,72,85,85,85,72,61,52,45,40,37,49,50,53,58,65,74,85,98,98,98,85,74,65,58,53,50,
49,50,53,58,65,74,85,98,98,98,85,74,65,58,53,50,49,50,53,58,65,74,85,98,98,98,85,74,65,58,53,50,
36,37,40,45,52,61,72,85,85,85,72,61,52,45,40,37,25,26,29,34,41,50,61,74,74,74,61,50,41,34,29,26,
16,17,20,25,32,41,52,65,65,65,52,41,32,25,20,17,9,10,13,18,25,34,45,58,58,58,45,34,25,18,13,10,
4,5,8,13,20,29,40,53,53,53,40,29,20,13,8,5,1,2,5,10,17,26,37,50,50,50,37,26,17,10,5,2
};

#define ICS_BUFF 30720000

unsigned short twoCompNibbleSquared[16] = { 0, 1, 4, 9, 16, 25, 36, 49, 64, 49, 36, 25, 16, 9, 4, 1  };

unsigned short absComplexNumber(char val) {
	return twoCompNibbleSquared[((val & 0xF0) >> 4)] + twoCompNibbleSquared[(val & 0x0F)];
}


int read_from_input(course_chan_input_matrix* matrix, course_chan_input_array* input)
{
	static bool first_run = true;

	if (first_run) {

		for (int i = 0; i < 4; i++)
			for (int j = 0; j < 8; j++)
				matrix->m_input_matrix[i][j].pad_input = true;


		for (int i = 0; i < 32; ++i) {

			// if there is no stream on this handle then just keep the buffer to all 0's
			if (input->m_handles[i].pad_input == true) {
				continue;
			}

			uint64_t read_in = 0;
			uint64_t total_read = 0;

			char* buff = (char*)malloc(PACKETS_PER_50MS * PACKET_SIZE_BYTES);
			if (buff == NULL)
				return errno;

			memset(buff, 0, sizeof(buff));

			while ((read_in = read(input->m_handles[i].m_handle, (buff + total_read), (PACKETS_PER_50MS * PACKET_SIZE_BYTES)-total_read)) > 0)
				total_read += read_in;

			if (total_read != (PACKETS_PER_50MS * PACKET_SIZE_BYTES))
				return errno;


			unsigned short w1 = (buff[1] << 8) | buff[0];
			unsigned short w2 = (buff[3] << 8) | buff[2];
			unsigned short w3 = (buff[5] << 8) | buff[4];

			unsigned char freq_index = (w2>>4) & 0x7;
			unsigned char rx_index = (w3>>14) & 0x3;

			printf("%s %04x %04x %04x %d %d\n", input->m_handles[i].m_id, w1, w2, w3, rx_index, freq_index);

			// Ensure we are in range
			assert(rx_index >= 0 && rx_index <= 3);
			assert(freq_index >= 0 && freq_index <= 7);

			matrix->m_input_matrix[rx_index][freq_index].m_handle = input->m_handles[i].m_handle;
			strcpy(matrix->m_input_matrix[rx_index][freq_index].m_id, input->m_handles[i].m_id);
			matrix->m_input_matrix[rx_index][freq_index].m_buff = buff;
			matrix->m_input_matrix[rx_index][freq_index].pad_input = false;
		}


		for (int i = 0; i < 4; i++)
			for (int j = 0; j < 8; j++)
				// if this entry in the matrix does not have a stream, then pad it's buffer with all zeros
				if (matrix->m_input_matrix[i][j].pad_input == true) {
					char* buff = (char*)malloc(PACKETS_PER_50MS * PACKET_SIZE_BYTES);
					if (buff == NULL)
						return errno;

					memset(buff, 0, sizeof(buff));

					matrix->m_input_matrix[i][j].m_buff = buff;
				}


		first_run = false;
	}
	else
	{
		for (int i = 0; i < 4; i++)
			for (int j = 0; j < 8; j++) {

				// don't want to read from the handle if there is no stream behind it
				if (matrix->m_input_matrix[i][j].pad_input == true)
					continue;

				uint64_t read_in = 0;
				uint64_t total_read = 0;

				while ((read_in = read(matrix->m_input_matrix[i][j].m_handle, (matrix->m_input_matrix[i][j].m_buff + total_read), (PACKETS_PER_50MS * PACKET_SIZE_BYTES)-total_read)) > 0)
					total_read += read_in;

				if (total_read != (PACKETS_PER_50MS * PACKET_SIZE_BYTES))
					return errno;
			}
	}

	return 0;
}


void course_channel_swap(const course_chan_freq* in, course_chan_freq* out, unsigned int* course_swap_index)
{
	unsigned int freq[24];

	memcpy(freq, in->m_freq, sizeof(freq));

	unsigned int temp;

	for(int i = 0; i < 24; i++)
	{
		for(int j = i; j < 24;j++)
		{
			if(freq[i] > freq[j])
			{
				temp = freq[i];
				freq[i] = freq[j];
				freq[j] = temp;
			}
		}
	}



	// no channels to swap
	*course_swap_index = 24;

	// find the index where the channels are swapped i.e. where 129 exists
	for (int i = 0; i < 24; ++i) {
		if (freq[i] >= 129) {
			*course_swap_index = i;
			break;
		}
	}


	// reorder freq array based on the course channel boundary around 129
	for (int i = 0; i < 24; ++i) {
		if (i < *course_swap_index)
			out->m_freq[i] = freq[i];
		else
			out->m_freq[23-i+(*course_swap_index)] = freq[i];
	}

}


int recombine(course_chan_input_array* input, course_chan_output_array* output, ics_handle* ics_out, unsigned int course_swap_index, bool skipics, bool skipcourse)
{
	course_chan_input_matrix inputs;

	memset(&inputs, 0, sizeof(course_chan_input_matrix));

	unsigned int header_offset = 6;

	// create file buffer
	char* file_buffer[24];
	uint64_t file_buffer_index[24];

	for (int b = 0; b < 24; b++) {
		file_buffer[b] = (char*)malloc(64000*256);

		if (file_buffer[b] == NULL)
			return errno;
	}

	char* ics_buffer = NULL;
	uint64_t ics_buffer_index = 0;

	if (!skipics) {
		ics_buffer = (char*)malloc(ICS_BUFF);
		if (ics_buffer == NULL)
			return errno;
	}

	unsigned short ics = 0;
	unsigned char ics_byte = 0;
	char* mem = NULL;

	int64_t t_sample_offset = 0;
	int64_t course_chan_offset = 0;
	int64_t freq_group_offset = 0;
	int64_t ten_kHz_offset = 0;

	// Read in 50ms chunks which is 48000 packets
	for (int ms = 0; ms < 20; ms++) {

		memset(file_buffer_index, 0, sizeof(file_buffer_index));

		int read = read_from_input(&inputs, input);
		if (read < 0)
			return errno;

		for (int t_sample = 0; t_sample < 500; ++t_sample) {
			t_sample_offset = t_sample * 264;

			for (int course_ch = 0; course_ch < 24; ++course_ch) {
				if (course_ch < course_swap_index)
					course_chan_offset = course_ch * 4 * 500 * 264;
				else
					course_chan_offset = (23-course_ch+course_swap_index) * 4 * 500 * 264;

				for (int lane_id = 0; lane_id < 8; ++lane_id) {

					for (int freq_grp = 0; freq_grp < 4; ++freq_grp) {
						freq_group_offset = freq_grp * 500 * 264;

						for (int ten_kHz = 0; ten_kHz < 4; ++ten_kHz) {
							ten_kHz_offset = ten_kHz * 64;

							ics = 0;

							for (int pfb_no = 0; pfb_no < 4; ++pfb_no) {

								// start of packet
								mem = inputs.m_input_matrix[pfb_no][lane_id].m_buff + t_sample_offset + course_chan_offset + freq_group_offset + ten_kHz_offset + header_offset;

								if (!skipcourse) {
									memcpy((file_buffer[course_ch]+file_buffer_index[course_ch]), mem, 64);
									file_buffer_index[course_ch] += 64;
								}

								if (!skipics) {
									for (unsigned int tile = 0; tile < 64; ++tile) {
										ics += byte_to_sum[(unsigned char)mem[tile]];
									}
								}

							} // end 4 lots of 64

							if (!skipics) {
								// normalisation: number of ants * num pols
								ics_byte = (unsigned char)(ics / 256);

								// copy ics 1 byte value into buffer
								memcpy(ics_buffer+ics_buffer_index, &ics_byte, sizeof(unsigned char));
								//ics_buffer[ics_buffer_index] = ics_byte;
								ics_buffer_index += sizeof(unsigned char);
							}

						}

					}
				}

			}

		}

		if (!skipcourse) {
			//write out each course channel
			for (int c = 0; c < 24; c++) {

				uint64_t written = 0;
				uint64_t total_written = 0;

				while ((written = write(output->m_handles[c].m_handle, file_buffer[c]+total_written, (64000*256)-total_written)) > 0)
					total_written += written;

				if (total_written != (64000*256))
					return errno;
			}
		}

	} // end 20ms chunks


	if (!skipics) {
		// write out ics buffers
		uint64_t written_ics = 0;
		uint64_t total_written_ics = 0;

		while ((written_ics = write(ics_out->m_handle, ics_buffer+total_written_ics, (ICS_BUFF)-total_written_ics)) > 0)
			total_written_ics += written_ics;

		if (total_written_ics != (ICS_BUFF))
			return errno;
	}

	for (int i = 0; i < 4; i++)
		for (int j = 0; j < 8; j++)
			free(inputs.m_input_matrix[i][j].m_buff);

	for (int b = 0; b < 24; b++)
		free(file_buffer[b]);

	if (!skipics)
		free(ics_buffer);

	return 0;
}

int open_input_from_socket(course_chan_input_array* input, unsigned short portno)
{
	int parentfd; /* parent socket */
	int childfd; /* child socket */
	unsigned int clientlen; /* byte size of client's address */
	struct sockaddr_in serveraddr; /* server's addr */
	struct sockaddr_in clientaddr; /* client addr */
	struct hostent *hostp; /* client host info */
	char *hostaddrp; /* dotted decimal host addr string */
	int optval; /* flag value for setsockopt */

	//socket: create the parent socket
	parentfd = socket(AF_INET, SOCK_STREAM, 0);
	if (parentfd < 0)
		return errno;

	/* setsockopt: Handy debugging trick that lets
	* us rerun the server immediately after we kill it;
	* otherwise we have to wait about 20 secs.
	* Eliminates "ERROR on binding: Address already in use" error.
	*/
	optval = 1;
	setsockopt(parentfd, SOL_SOCKET, SO_REUSEADDR, (const void *)&optval , sizeof(int));


	//build the server's Internet address
	bzero((char *) &serveraddr, sizeof(serveraddr));

	//this is an Internet address
	serveraddr.sin_family = AF_INET;

	// let the system figure out our IP address
	serveraddr.sin_addr.s_addr = htonl(INADDR_ANY);

	// this is the port we will listen on
	serveraddr.sin_port = htons((unsigned short)portno);

	//bind: associate the parent socket with a port
	if (bind(parentfd, (struct sockaddr *) &serveraddr, sizeof(serveraddr)) < 0)
		return errno;

	//listen: make this socket ready to accept connection requests

	if (listen(parentfd, 32) < 0)
		return errno;


	clientlen = sizeof(clientaddr);

	// We only want 32 connection inputs
	for (int i = 0; i < 32; ++i) {

		 //accept: wait for a connection request
		childfd = accept(parentfd, (struct sockaddr *) &clientaddr, &clientlen);
		if (childfd < 0)
		  return errno;

		 //gethostbyaddr: determine who sent the message
		hostp = gethostbyaddr((const char *)&clientaddr.sin_addr.s_addr, sizeof(clientaddr.sin_addr.s_addr), AF_INET);
		if (hostp == NULL)
		  return errno;

		hostaddrp = inet_ntoa(clientaddr.sin_addr);
		if (hostaddrp == NULL)
		  return errno;

		printf("Established connection with %s (%s)\n", hostp->h_name, hostaddrp);

		// populate
		input->m_handles[i].m_handle = childfd;
		strcpy(input->m_handles[i].m_id, hostp->h_name);
	}

	return 0;
}


int open_input_from_directory(const char* directory, course_chan_input_array* input)
{
	unsigned int count = 0;

	struct dirent *ep;

	DIR* dp = opendir(directory);

	if (dp != NULL) {
		while ( (ep = readdir(dp)) ) {
			std::string name(ep->d_name);
			if (name == "." || name == "..")
				continue;

			strcpy(input->m_handles[count].m_id, ep->d_name);

			std::string full = std::string(directory) + "/" + name;
			if ((input->m_handles[count].m_handle = open(full.c_str(), O_RDONLY)) < 0)
				input->m_handles[count].pad_input = true; // input failed to open; pad stream with zeros

			count+=1;

			// we can not exceed 32 input files
			if (count >= 32)
				break;

		}

		closedir(dp);
	}
	else {
		perror("Couldn't open the directory\n");
		return errno;
	}

	return 0;
}


int open_input_from_file_list(const char* input_file_list, course_chan_input_array* input)
{
	FILE *fp;
	char *line = NULL;
	size_t len = 0;
	ssize_t read;
	unsigned int count = 0;

	fp = fopen(input_file_list, "r");
	if (fp == NULL)
		exit(EXIT_FAILURE);

	while ((read = getline(&line, &len, fp)) != -1 && count < 32) {

		if (line[strlen(line) - 1] == '\n')
			line[strlen(line)-1] = 0;

		strcpy(input->m_handles[count].m_id, line);

		count += 1;
	}

	free(line);

	return open_input_from_file(input);
}


int open_input_from_file(course_chan_input_array* input)
{
	for (int i = 0; i < 32; ++i)
		if ((input->m_handles[i].m_handle = open(input->m_handles[i].m_id, O_RDONLY)) < 0)
			input->m_handles[i].pad_input = true; // input failed to open; pad stream with zeros

	return 0;
}


int open_output_to_file(course_chan_output_array* output)
{
	for (int i = 0; i < 24; i++)
		if ((output->m_handles[i].m_handle = open(output->m_handles[i].m_id, O_WRONLY | O_CREAT | O_TRUNC, S_IRWXU)) < 0)
			return errno;

	return 0;
}


void close_input_handles(course_chan_input_array* input)
{
	int ret = 0;
	for (int i = 0; i < 24; ++i) {
		ret = close(input->m_handles[i].m_handle);
	}
}

void close_output_handles(course_chan_output_array* output)
{
	int ret = 0;
	for (int i = 0; i < 24; ++i) {
		ret = close(output->m_handles[i].m_handle);
	}
}

