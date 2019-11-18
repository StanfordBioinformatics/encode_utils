import encode_utils as eu
from encode_utils.connection import Connection
import urllib.request #python3
# import urllib #python2
import argparse
import os
import requests
import subprocess
import mimetypes
from PIL import Image
from base64 import b64encode
import magic  # install me with 'pip install python-magic'



conn = Connection("dev")

def attachment(path):
    """ Create an attachment upload object from a filename
    Embeds the attachment as a data url.
    """

    filename = os.path.basename(path)
    print(filename)
    mime_type, encoding = mimetypes.guess_type(path)
    print(mime_type, encoding)
    major, minor = mime_type.split('/')
    # detected_type = magic.from_file(path, mime=True).decode('ascii')
    detected_type = magic.from_file(path, mime=True)

    # XXX This validation logic should move server-side.
    if not (detected_type == mime_type or
            detected_type == 'text/plain' and major == 'text'):
        raise ValueError('Wrong extension for %s: %s' % (detected_type, filename))

    with open(path, 'rb') as stream:
        attach = {
            'download': filename,
            'type': mime_type,
            'href': 'data:%s;base64,%s' % (mime_type, b64encode(stream.read()).decode('ascii'))
        }

        if mime_type in ('application/pdf', 'text/plain', 'text/tab-separated-values', 'text/html'):
            # XXX Should use chardet to detect charset for text files here.
            return attach

        if major == 'image' and minor in ('png', 'jpeg', 'gif', 'tiff'):
            # XXX we should just convert our tiffs to pngs
            stream.seek(0, 0)
            im = Image.open(stream)
            im.verify()
            if im.format != minor.upper():
                msg = "Image file format %r does not match extension for %s"
                raise ValueError(msg % (im.format, filename))

            attach['width'], attach['height'] = im.size
            return attach

    raise ValueError("Unknown file type for %s" % filename)

req = urllib.request.urlretrieve("https://encodeproject.org/biosample-characterizations/c97cfba6-49ac-48ec-a6a9-d7582a0f7be9/@@download/attachment/KMT2B-FLAG_HepG2_PCR_validation.jpg", "KMT2B-FLAG_HepG2_PCR_validation.jpg")

#python2
# req = urllib.urlretrieve("https://encodeproject.org/biosample-characterizations/c97cfba6-49ac-48ec-a6a9-d7582a0f7be9/@@download/attachment/KMT2B-FLAG_HepG2_PCR_validation.jpg")
print('req = ', req)
# attach = attachment(req[0])
cwd = os.getcwd()
print('cwd type= ', type(cwd))
print('cwd =', cwd)
attach = attachment(cwd + "/KMT2B-FLAG_HepG2_PCR_validation.jpg")

payload = {
	"attachment": attach,
    "award": "/awards/UM1HG009411/",
    "characterizes": "/genetic-modifications/TSTGM121838/",
    "lab": "/labs/richard-myers/"
}

payload[Connection.PROFILE_KEY] = "genetic_modification_characterization"
var = conn.post(payload, require_aliases=False)
print(var)
